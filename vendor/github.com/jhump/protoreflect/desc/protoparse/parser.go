package protoparse

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/internal"
)

//go:generate goyacc -o proto.y.go -p proto proto.y

func init() {
	protoErrorVerbose = true

	// fix up the generated "token name" array so that error messages are nicer
	setTokenName(_STRING_LIT, "string literal")
	setTokenName(_INT_LIT, "int literal")
	setTokenName(_FLOAT_LIT, "float literal")
	setTokenName(_NAME, "identifier")
	setTokenName(_ERROR, "error")
	// for keywords, just show the keyword itself wrapped in quotes
	for str, i := range keywords {
		setTokenName(i, fmt.Sprintf(`"%s"`, str))
	}
}

func setTokenName(token int, text string) {
	// NB: this is based on logic in generated parse code that translates the
	// int returned from the lexer into an internal token number.
	var intern int
	if token < len(protoTok1) {
		intern = protoTok1[token]
	} else {
		if token >= protoPrivate {
			if token < protoPrivate+len(protoTok2) {
				intern = protoTok2[token-protoPrivate]
			}
		}
		if intern == 0 {
			for i := 0; i+1 < len(protoTok3); i += 2 {
				if protoTok3[i] == token {
					intern = protoTok3[i+1]
					break
				}
			}
		}
	}

	if intern >= 1 && intern-1 < len(protoToknames) {
		protoToknames[intern-1] = text
		return
	}

	panic(fmt.Sprintf("Unknown token value: %d", token))
}

// FileAccessor is an abstraction for opening proto source files. It takes the
// name of the file to open and returns either the input reader or an error.
type FileAccessor func(filename string) (io.ReadCloser, error)

// FileContentsFromMap returns a FileAccessor that uses the given map of file
// contents. This allows proto source files to be constructed in memory and
// easily supplied to a parser. The map keys are the paths to the proto source
// files, and the values are the actual proto source contents.
func FileContentsFromMap(files map[string]string) FileAccessor {
	return func(filename string) (io.ReadCloser, error) {
		contents, ok := files[filename]
		if !ok {
			return nil, os.ErrNotExist
		}
		return ioutil.NopCloser(strings.NewReader(contents)), nil
	}
}

// Parser parses proto source into descriptors.
type Parser struct {
	// The paths used to search for dependencies that are referenced in import
	// statements in proto source files. If no import paths are provided then
	// "." (current directory) is assumed to be the only import path.
	//
	// This setting is only used during ParseFiles operations. Since calls to
	// ParseFilesButDoNotLink do not link, there is no need to load and parse
	// dependencies.
	ImportPaths []string

	// If true, the supplied file names/paths need not necessarily match how the
	// files are referenced in import statements. The parser will attempt to
	// match import statements to supplied paths, "guessing" the import paths
	// for the files. Note that this inference is not perfect and link errors
	// could result. It works best when all proto files are organized such that
	// a single import path can be inferred (e.g. all files under a single tree
	// with import statements all being relative to the root of this tree).
	InferImportPaths bool

	// Used to create a reader for a given filename, when loading proto source
	// file contents. If unset, os.Open is used. If ImportPaths is also empty
	// then relative paths are will be relative to the process's current working
	// directory.
	Accessor FileAccessor

	// If true, the resulting file descriptors will retain source code info,
	// that maps elements to their location in the source files as well as
	// includes comments found during parsing (and attributed to elements of
	// the source file).
	IncludeSourceCodeInfo bool

	// If true, the results from ParseFilesButDoNotLink will be passed through
	// some additional validations. But only constraints that do not require
	// linking can be checked. These include proto2 vs. proto3 language features,
	// looking for incorrect usage of reserved names or tags, and ensuring that
	// fields have unique tags and that enum values have unique numbers (unless
	// the enum allows aliases).
	ValidateUnlinkedFiles bool

	// If true, the results from ParseFilesButDoNotLink will have options
	// interpreted. Any uninterpretable options (including any custom options or
	// options that refer to message and enum types, which can only be
	// interpreted after linking) will be left in uninterpreted_options. Also,
	// the "default" pseudo-option for fields can only be interpreted for scalar
	// fields, excluding enums. (Interpreting default values for enum fields
	// requires resolving enum names, which requires linking.)
	InterpretOptionsInUnlinkedFiles bool

	// A custom reporter of syntax and link errors. If not specified, the
	// default reporter just returns the reported error, which causes parsing
	// to abort after encountering a single error.
	//
	// The reporter is not invoked for system or I/O errors, only for syntax and
	// link errors.
	ErrorReporter ErrorReporter
}

// ParseFiles parses the named files into descriptors. The returned slice has
// the same number of entries as the give filenames, in the same order. So the
// first returned descriptor corresponds to the first given name, and so on.
//
// All dependencies for all specified files (including transitive dependencies)
// must be accessible via the parser's Accessor or a link error will occur. The
// exception to this rule is that files can import standard Google-provided
// files -- e.g. google/protobuf/*.proto -- without needing to supply sources
// for these files. Like protoc, this parser has a built-in version of these
// files it can use if they aren't explicitly supplied.
//
// If the Parser has no ErrorReporter set and a syntax or link error occurs,
// parsing will abort with the first such error encountered. If there is an
// ErrorReporter configured and it returns non-nil, parsing will abort with the
// error it returns. If syntax or link errors are encountered but the configured
// ErrorReporter always returns nil, the parse fails with ErrInvalidSource.
func (p Parser) ParseFiles(filenames ...string) ([]*desc.FileDescriptor, error) {
	accessor := p.Accessor
	if accessor == nil {
		accessor = func(name string) (io.ReadCloser, error) {
			return os.Open(name)
		}
	}
	paths := p.ImportPaths
	if len(paths) > 0 {
		acc := accessor
		accessor = func(name string) (io.ReadCloser, error) {
			var ret error
			for _, path := range paths {
				f, err := acc(filepath.Join(path, name))
				if err != nil {
					if ret == nil {
						ret = err
					}
					continue
				}
				return f, nil
			}
			return nil, ret
		}
	}

	protos := map[string]*parseResult{}
	results := &parseResults{resultsByFilename: protos}
	errs := newErrorHandler(p.ErrorReporter)
	parseProtoFiles(accessor, filenames, errs, true, true, results)
	if err := errs.getError(); err != nil {
		return nil, err
	}
	if p.InferImportPaths {
		// TODO: if this re-writes one of the names in filenames, lookups below will break
		protos = fixupFilenames(protos)
	}
	linkedProtos, err := newLinker(results, errs).linkFiles()
	if err != nil {
		return nil, err
	}
	if p.IncludeSourceCodeInfo {
		for name, fd := range linkedProtos {
			pr := protos[name]
			fd.AsFileDescriptorProto().SourceCodeInfo = pr.generateSourceCodeInfo()
			internal.RecomputeSourceInfo(fd)
		}
	}
	fds := make([]*desc.FileDescriptor, len(filenames))
	for i, name := range filenames {
		fd := linkedProtos[name]
		fds[i] = fd
	}
	return fds, nil
}

// ParseFilesButDoNotLink parses the named files into descriptor protos. The
// results are just protos, not fully-linked descriptors. It is possible that
// descriptors are invalid and still be returned in parsed form without error
// due to the fact that the linking step is skipped (and thus many validation
// steps omitted).
//
// There are a few side effects to not linking the descriptors:
//   1. No options will be interpreted. Options can refer to extensions or have
//      message and enum types. Without linking, these extension and type
//      references are not resolved, so the options may not be interpretable.
//      So all options will appear in UninterpretedOption fields of the various
//      descriptor options messages.
//   2. Type references will not be resolved. This means that the actual type
//      names in the descriptors may be unqualified and even relative to the
//      scope in which the type reference appears. This goes for fields that
//      have message and enum types. It also applies to methods and their
//      references to request and response message types.
//   3. Enum fields are not known. Until a field's type reference is resolved
//      (during linking), it is not known whether the type refers to a message
//      or an enum. So all fields with such type references have their Type set
//      to TYPE_MESSAGE.
//
// This method will still validate the syntax of parsed files. If the parser's
// ValidateUnlinkedFiles field is true, additional checks, beyond syntax will
// also be performed.
//
// If the Parser has no ErrorReporter set and a syntax or link error occurs,
// parsing will abort with the first such error encountered. If there is an
// ErrorReporter configured and it returns non-nil, parsing will abort with the
// error it returns. If syntax or link errors are encountered but the configured
// ErrorReporter always returns nil, the parse fails with ErrInvalidSource.
func (p Parser) ParseFilesButDoNotLink(filenames ...string) ([]*dpb.FileDescriptorProto, error) {
	accessor := p.Accessor
	if accessor == nil {
		accessor = func(name string) (io.ReadCloser, error) {
			return os.Open(name)
		}
	}

	protos := map[string]*parseResult{}
	errs := newErrorHandler(p.ErrorReporter)
	parseProtoFiles(accessor, filenames, errs, false, p.ValidateUnlinkedFiles, &parseResults{resultsByFilename: protos})
	if err := errs.getError(); err != nil {
		return nil, err
	}
	if p.InferImportPaths {
		// TODO: if this re-writes one of the names in filenames, lookups below will break
		protos = fixupFilenames(protos)
	}
	fds := make([]*dpb.FileDescriptorProto, len(filenames))
	for i, name := range filenames {
		pr := protos[name]
		fd := pr.fd
		if p.InterpretOptionsInUnlinkedFiles {
			pr.lenient = true
			_ = interpretFileOptions(pr, poorFileDescriptorish{FileDescriptorProto: fd})
		}
		if p.IncludeSourceCodeInfo {
			fd.SourceCodeInfo = pr.generateSourceCodeInfo()
		}
		fds[i] = fd
	}
	return fds, nil
}

func fixupFilenames(protos map[string]*parseResult) map[string]*parseResult {
	// In the event that the given filenames (keys in the supplied map) do not
	// match the actual paths used in 'import' statements in the files, we try
	// to revise names in the protos so that they will match and be linkable.
	revisedProtos := map[string]*parseResult{}

	protoPaths := map[string]struct{}{}
	// TODO: this is O(n^2) but could likely be O(n) with a clever data structure (prefix tree that is indexed backwards?)
	importCandidates := map[string]map[string]struct{}{}
	candidatesAvailable := map[string]struct{}{}
	for name := range protos {
		candidatesAvailable[name] = struct{}{}
		for _, f := range protos {
			for _, imp := range f.fd.Dependency {
				if strings.HasSuffix(name, imp) {
					candidates := importCandidates[imp]
					if candidates == nil {
						candidates = map[string]struct{}{}
						importCandidates[imp] = candidates
					}
					candidates[name] = struct{}{}
				}
			}
		}
	}
	for imp, candidates := range importCandidates {
		// if we found multiple possible candidates, use the one that is an exact match
		// if it exists, and otherwise, guess that it's the shortest path (fewest elements)
		var best string
		for c := range candidates {
			if _, ok := candidatesAvailable[c]; !ok {
				// already used this candidate and re-written its filename accordingly
				continue
			}
			if c == imp {
				// exact match!
				best = c
				break
			}
			if best == "" {
				best = c
			} else {
				// HACK: we can't actually tell which files is supposed to match
				// this import, so arbitrarily pick the "shorter" one (fewest
				// path elements) or, on a tie, the lexically earlier one
				minLen := strings.Count(best, string(filepath.Separator))
				cLen := strings.Count(c, string(filepath.Separator))
				if cLen < minLen || (cLen == minLen && c < best) {
					best = c
				}
			}
		}
		if best != "" {
			prefix := best[:len(best)-len(imp)]
			if len(prefix) > 0 {
				protoPaths[prefix] = struct{}{}
			}
			f := protos[best]
			f.fd.Name = proto.String(imp)
			revisedProtos[imp] = f
			delete(candidatesAvailable, best)
		}
	}

	if len(candidatesAvailable) == 0 {
		return revisedProtos
	}

	if len(protoPaths) == 0 {
		for c := range candidatesAvailable {
			revisedProtos[c] = protos[c]
		}
		return revisedProtos
	}

	// Any remaining candidates are entry-points (not imported by others), so
	// the best bet to "fixing" their file name is to see if they're in one of
	// the proto paths we found, and if so strip that prefix.
	protoPathStrs := make([]string, len(protoPaths))
	i := 0
	for p := range protoPaths {
		protoPathStrs[i] = p
		i++
	}
	sort.Strings(protoPathStrs)
	// we look at paths in reverse order, so we'll use a longer proto path if
	// there is more than one match
	for c := range candidatesAvailable {
		var imp string
		for i := len(protoPathStrs) - 1; i >= 0; i-- {
			p := protoPathStrs[i]
			if strings.HasPrefix(c, p) {
				imp = c[len(p):]
				break
			}
		}
		if imp != "" {
			f := protos[c]
			f.fd.Name = proto.String(imp)
			revisedProtos[imp] = f
		} else {
			revisedProtos[c] = protos[c]
		}
	}

	return revisedProtos
}

func parseProtoFiles(acc FileAccessor, filenames []string, errs *errorHandler, recursive, validate bool, parsed *parseResults) {
	for _, name := range filenames {
		parseProtoFile(acc, name, nil, errs, recursive, validate, parsed)
		if errs.err != nil {
			return
		}
	}
}

func parseProtoFile(acc FileAccessor, filename string, importLoc *SourcePos, errs *errorHandler, recursive, validate bool, parsed *parseResults) {
	if parsed.has(filename) {
		return
	}
	in, err := acc(filename)
	var result *parseResult
	if err == nil {
		// try to parse the bytes accessed
		func() {
			defer func() {
				// if we've already parsed contents, an error
				// closing need not fail this operation
				_ = in.Close()
			}()
			result = parseProto(filename, in, errs, validate)
		}()
	} else if d, ok := standardImports[filename]; ok {
		// it's a well-known import
		// (we clone it to make sure we're not sharing state with other
		//  parsers, which could result in unsafe races if multiple
		//  parsers are trying to access it concurrently)
		result = &parseResult{fd: proto.Clone(d).(*dpb.FileDescriptorProto)}
	} else {
		if !strings.Contains(err.Error(), filename) {
			// an error message that doesn't indicate the file is awful!
			err = fmt.Errorf("%s: %v", filename, err)
		}
		if _, ok := err.(ErrorWithPos); !ok && importLoc != nil {
			// error has no source position? report it as the import line
			err = ErrorWithSourcePos{
				Pos:        importLoc,
				Underlying: err,
			}
		}
		_ = errs.handleError(err)
		return
	}

	parsed.add(filename, result)

	if errs.getError() != nil {
		return // abort
	}

	if recursive {
		fd := result.fd
		decl := result.getFileNode(fd)
		fnode, ok := decl.(*fileNode)
		if !ok {
			// no AST for this file? use imports in descriptor
			for _, dep := range fd.Dependency {
				parseProtoFile(acc, dep, decl.start(), errs, true, validate, parsed)
				if errs.getError() != nil {
					return // abort
				}
			}
			return
		}
		// we have an AST; use it so we can report import location in errors
		for _, dep := range fnode.imports {
			parseProtoFile(acc, dep.name.val, dep.name.start(), errs, true, validate, parsed)
			if errs.getError() != nil {
				return // abort
			}
		}
	}
}

type parseResults struct {
	resultsByFilename map[string]*parseResult
	filenames         []string
}

func (r *parseResults) has(filename string) bool {
	_, ok := r.resultsByFilename[filename]
	return ok
}

func (r *parseResults) add(filename string, result *parseResult) {
	r.resultsByFilename[filename] = result
	r.filenames = append(r.filenames, filename)
}

type parseResult struct {
	// handles any errors encountered during parsing, construction of file descriptor,
	// or validation
	errs *errorHandler

	// the parsed file descriptor
	fd *dpb.FileDescriptorProto

	// if set to true, enables lenient interpretation of options, where
	// unrecognized options will be left uninterpreted instead of resulting in a
	// link error
	lenient bool

	// a map of elements in the descriptor to nodes in the AST
	// (for extracting position information when validating the descriptor)
	nodes map[proto.Message]node

	// a map of uninterpreted option AST nodes to their relative path
	// in the resulting options message
	interpretedOptions map[*optionNode][]int32
}

func (r *parseResult) getFileNode(f *dpb.FileDescriptorProto) fileDecl {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(f.GetName())}
	}
	return r.nodes[f].(fileDecl)
}

func (r *parseResult) getOptionNode(o *dpb.UninterpretedOption) optionDecl {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(r.fd.GetName())}
	}
	return r.nodes[o].(optionDecl)
}

func (r *parseResult) getOptionNamePartNode(o *dpb.UninterpretedOption_NamePart) node {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(r.fd.GetName())}
	}
	return r.nodes[o]
}

func (r *parseResult) getFieldNode(f *dpb.FieldDescriptorProto) fieldDecl {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(r.fd.GetName())}
	}
	return r.nodes[f].(fieldDecl)
}

func (r *parseResult) getExtensionRangeNode(e *dpb.DescriptorProto_ExtensionRange) rangeDecl {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(r.fd.GetName())}
	}
	return r.nodes[e].(rangeDecl)
}

func (r *parseResult) getMessageReservedRangeNode(rr *dpb.DescriptorProto_ReservedRange) rangeDecl {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(r.fd.GetName())}
	}
	return r.nodes[rr].(rangeDecl)
}

func (r *parseResult) getEnumValueNode(e *dpb.EnumValueDescriptorProto) enumValueDecl {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(r.fd.GetName())}
	}
	return r.nodes[e].(enumValueDecl)
}

func (r *parseResult) getEnumReservedRangeNode(rr *dpb.EnumDescriptorProto_EnumReservedRange) rangeDecl {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(r.fd.GetName())}
	}
	return r.nodes[rr].(rangeDecl)
}

func (r *parseResult) getMethodNode(m *dpb.MethodDescriptorProto) methodDecl {
	if r.nodes == nil {
		return noSourceNode{pos: unknownPos(r.fd.GetName())}
	}
	return r.nodes[m].(methodDecl)
}

func (r *parseResult) putFileNode(f *dpb.FileDescriptorProto, n *fileNode) {
	r.nodes[f] = n
}

func (r *parseResult) putOptionNode(o *dpb.UninterpretedOption, n *optionNode) {
	r.nodes[o] = n
}

func (r *parseResult) putOptionNamePartNode(o *dpb.UninterpretedOption_NamePart, n *optionNamePartNode) {
	r.nodes[o] = n
}

func (r *parseResult) putMessageNode(m *dpb.DescriptorProto, n msgDecl) {
	r.nodes[m] = n
}

func (r *parseResult) putFieldNode(f *dpb.FieldDescriptorProto, n fieldDecl) {
	r.nodes[f] = n
}

func (r *parseResult) putOneOfNode(o *dpb.OneofDescriptorProto, n *oneOfNode) {
	r.nodes[o] = n
}

func (r *parseResult) putExtensionRangeNode(e *dpb.DescriptorProto_ExtensionRange, n *rangeNode) {
	r.nodes[e] = n
}

func (r *parseResult) putMessageReservedRangeNode(rr *dpb.DescriptorProto_ReservedRange, n *rangeNode) {
	r.nodes[rr] = n
}

func (r *parseResult) putEnumNode(e *dpb.EnumDescriptorProto, n *enumNode) {
	r.nodes[e] = n
}

func (r *parseResult) putEnumValueNode(e *dpb.EnumValueDescriptorProto, n *enumValueNode) {
	r.nodes[e] = n
}

func (r *parseResult) putEnumReservedRangeNode(rr *dpb.EnumDescriptorProto_EnumReservedRange, n *rangeNode) {
	r.nodes[rr] = n
}

func (r *parseResult) putServiceNode(s *dpb.ServiceDescriptorProto, n *serviceNode) {
	r.nodes[s] = n
}

func (r *parseResult) putMethodNode(m *dpb.MethodDescriptorProto, n *methodNode) {
	r.nodes[m] = n
}

func parseProto(filename string, r io.Reader, errs *errorHandler, validate bool) *parseResult {
	lx := newLexer(r, filename, errs)
	protoParse(lx)

	res := createParseResult(filename, lx.res, errs)
	if validate {
		basicValidate(res)
	}

	return res
}

func createParseResult(filename string, file *fileNode, errs *errorHandler) *parseResult {
	res := &parseResult{
		errs:               errs,
		nodes:              map[proto.Message]node{},
		interpretedOptions: map[*optionNode][]int32{},
	}
	if file == nil {
		// nil AST means there was an error that prevented any parsing
		// or the file was empty; synthesize empty non-nil AST
		file = &fileNode{}
		n := noSourceNode{pos: unknownPos(filename)}
		file.setRange(&n, &n)
	}
	res.createFileDescriptor(filename, file)
	return res
}

func (r *parseResult) createFileDescriptor(filename string, file *fileNode) {
	fd := &dpb.FileDescriptorProto{Name: proto.String(filename)}
	r.fd = fd
	r.putFileNode(fd, file)

	isProto3 := false
	if file.syntax != nil {
		isProto3 = file.syntax.syntax.val == "proto3"
		// proto2 is the default, so no need to set unless proto3
		if isProto3 {
			fd.Syntax = proto.String(file.syntax.syntax.val)
		}
	}

	for _, decl := range file.decls {
		if decl.enum != nil {
			fd.EnumType = append(fd.EnumType, r.asEnumDescriptor(decl.enum))
		} else if decl.extend != nil {
			r.addExtensions(decl.extend, &fd.Extension, &fd.MessageType, isProto3)
		} else if decl.imp != nil {
			file.imports = append(file.imports, decl.imp)
			index := len(fd.Dependency)
			fd.Dependency = append(fd.Dependency, decl.imp.name.val)
			if decl.imp.public {
				fd.PublicDependency = append(fd.PublicDependency, int32(index))
			} else if decl.imp.weak {
				fd.WeakDependency = append(fd.WeakDependency, int32(index))
			}
		} else if decl.message != nil {
			fd.MessageType = append(fd.MessageType, r.asMessageDescriptor(decl.message, isProto3))
		} else if decl.option != nil {
			if fd.Options == nil {
				fd.Options = &dpb.FileOptions{}
			}
			fd.Options.UninterpretedOption = append(fd.Options.UninterpretedOption, r.asUninterpretedOption(decl.option))
		} else if decl.service != nil {
			fd.Service = append(fd.Service, r.asServiceDescriptor(decl.service))
		} else if decl.pkg != nil {
			if fd.Package != nil {
				if r.errs.handleError(ErrorWithSourcePos{Pos: decl.pkg.start(), Underlying: errors.New("files should have only one package declaration")}) != nil {
					return
				}
			}
			fd.Package = proto.String(decl.pkg.name.val)
		}
	}
}

func (r *parseResult) asUninterpretedOptions(nodes []*optionNode) []*dpb.UninterpretedOption {
	if len(nodes) == 0 {
		return nil
	}
	opts := make([]*dpb.UninterpretedOption, len(nodes))
	for i, n := range nodes {
		opts[i] = r.asUninterpretedOption(n)
	}
	return opts
}

func (r *parseResult) asUninterpretedOption(node *optionNode) *dpb.UninterpretedOption {
	opt := &dpb.UninterpretedOption{Name: r.asUninterpretedOptionName(node.name.parts)}
	r.putOptionNode(opt, node)

	switch val := node.val.value().(type) {
	case bool:
		if val {
			opt.IdentifierValue = proto.String("true")
		} else {
			opt.IdentifierValue = proto.String("false")
		}
	case int64:
		opt.NegativeIntValue = proto.Int64(val)
	case uint64:
		opt.PositiveIntValue = proto.Uint64(val)
	case float64:
		opt.DoubleValue = proto.Float64(val)
	case string:
		opt.StringValue = []byte(val)
	case identifier:
		opt.IdentifierValue = proto.String(string(val))
	case []*aggregateEntryNode:
		var buf bytes.Buffer
		aggToString(val, &buf)
		aggStr := buf.String()
		opt.AggregateValue = proto.String(aggStr)
	}
	return opt
}

func (r *parseResult) asUninterpretedOptionName(parts []*optionNamePartNode) []*dpb.UninterpretedOption_NamePart {
	ret := make([]*dpb.UninterpretedOption_NamePart, len(parts))
	for i, part := range parts {
		txt := part.text.val
		if !part.isExtension {
			txt = part.text.val[part.offset : part.offset+part.length]
		}
		np := &dpb.UninterpretedOption_NamePart{
			NamePart:    proto.String(txt),
			IsExtension: proto.Bool(part.isExtension),
		}
		r.putOptionNamePartNode(np, part)
		ret[i] = np
	}
	return ret
}

func (r *parseResult) addExtensions(ext *extendNode, flds *[]*dpb.FieldDescriptorProto, msgs *[]*dpb.DescriptorProto, isProto3 bool) {
	extendee := ext.extendee.val
	for _, decl := range ext.decls {
		if decl.field != nil {
			decl.field.extendee = ext
			fd := r.asFieldDescriptor(decl.field)
			fd.Extendee = proto.String(extendee)
			*flds = append(*flds, fd)
		} else if decl.group != nil {
			decl.group.extendee = ext
			fd, md := r.asGroupDescriptors(decl.group, isProto3)
			fd.Extendee = proto.String(extendee)
			*flds = append(*flds, fd)
			*msgs = append(*msgs, md)
		}
	}
}

func asLabel(lbl *fieldLabel) *dpb.FieldDescriptorProto_Label {
	if lbl.identNode == nil {
		return nil
	}
	switch {
	case lbl.repeated:
		return dpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	case lbl.required:
		return dpb.FieldDescriptorProto_LABEL_REQUIRED.Enum()
	default:
		return dpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	}
}

func (r *parseResult) asFieldDescriptor(node *fieldNode) *dpb.FieldDescriptorProto {
	fd := newFieldDescriptor(node.name.val, node.fldType.val, int32(node.tag.val), asLabel(&node.label))
	r.putFieldNode(fd, node)
	if opts := node.options.Elements(); len(opts) > 0 {
		fd.Options = &dpb.FieldOptions{UninterpretedOption: r.asUninterpretedOptions(opts)}
	}
	return fd
}

var fieldTypes = map[string]dpb.FieldDescriptorProto_Type{
	"double":   dpb.FieldDescriptorProto_TYPE_DOUBLE,
	"float":    dpb.FieldDescriptorProto_TYPE_FLOAT,
	"int32":    dpb.FieldDescriptorProto_TYPE_INT32,
	"int64":    dpb.FieldDescriptorProto_TYPE_INT64,
	"uint32":   dpb.FieldDescriptorProto_TYPE_UINT32,
	"uint64":   dpb.FieldDescriptorProto_TYPE_UINT64,
	"sint32":   dpb.FieldDescriptorProto_TYPE_SINT32,
	"sint64":   dpb.FieldDescriptorProto_TYPE_SINT64,
	"fixed32":  dpb.FieldDescriptorProto_TYPE_FIXED32,
	"fixed64":  dpb.FieldDescriptorProto_TYPE_FIXED64,
	"sfixed32": dpb.FieldDescriptorProto_TYPE_SFIXED32,
	"sfixed64": dpb.FieldDescriptorProto_TYPE_SFIXED64,
	"bool":     dpb.FieldDescriptorProto_TYPE_BOOL,
	"string":   dpb.FieldDescriptorProto_TYPE_STRING,
	"bytes":    dpb.FieldDescriptorProto_TYPE_BYTES,
}

func newFieldDescriptor(name string, fieldType string, tag int32, lbl *dpb.FieldDescriptorProto_Label) *dpb.FieldDescriptorProto {
	fd := &dpb.FieldDescriptorProto{
		Name:     proto.String(name),
		JsonName: proto.String(internal.JsonName(name)),
		Number:   proto.Int32(tag),
		Label:    lbl,
	}
	t, ok := fieldTypes[fieldType]
	if ok {
		fd.Type = t.Enum()
	} else {
		// NB: we don't have enough info to determine whether this is an enum
		// or a message type, so we'll leave Type nil and set it later
		// (during linking)
		fd.TypeName = proto.String(fieldType)
	}
	return fd
}

func (r *parseResult) asGroupDescriptors(group *groupNode, isProto3 bool) (*dpb.FieldDescriptorProto, *dpb.DescriptorProto) {
	fieldName := strings.ToLower(group.name.val)
	fd := &dpb.FieldDescriptorProto{
		Name:     proto.String(fieldName),
		JsonName: proto.String(internal.JsonName(fieldName)),
		Number:   proto.Int32(int32(group.tag.val)),
		Label:    asLabel(&group.label),
		Type:     dpb.FieldDescriptorProto_TYPE_GROUP.Enum(),
		TypeName: proto.String(group.name.val),
	}
	r.putFieldNode(fd, group)
	md := &dpb.DescriptorProto{Name: proto.String(group.name.val)}
	r.putMessageNode(md, group)
	r.addMessageDecls(md, group.decls, isProto3)
	return fd, md
}

func (r *parseResult) asMapDescriptors(mapField *mapFieldNode, isProto3 bool) (*dpb.FieldDescriptorProto, *dpb.DescriptorProto) {
	var lbl *dpb.FieldDescriptorProto_Label
	if !isProto3 {
		lbl = dpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	}
	keyFd := newFieldDescriptor("key", mapField.mapType.keyType.val, 1, lbl)
	r.putFieldNode(keyFd, mapField.keyField())
	valFd := newFieldDescriptor("value", mapField.mapType.valueType.val, 2, lbl)
	r.putFieldNode(valFd, mapField.valueField())
	entryName := internal.InitCap(internal.JsonName(mapField.name.val)) + "Entry"
	fd := newFieldDescriptor(mapField.name.val, entryName, int32(mapField.tag.val), dpb.FieldDescriptorProto_LABEL_REPEATED.Enum())
	if opts := mapField.options.Elements(); len(opts) > 0 {
		fd.Options = &dpb.FieldOptions{UninterpretedOption: r.asUninterpretedOptions(opts)}
	}
	r.putFieldNode(fd, mapField)
	md := &dpb.DescriptorProto{
		Name:    proto.String(entryName),
		Options: &dpb.MessageOptions{MapEntry: proto.Bool(true)},
		Field:   []*dpb.FieldDescriptorProto{keyFd, valFd},
	}
	r.putMessageNode(md, mapField)
	return fd, md
}

func (r *parseResult) asExtensionRanges(node *extensionRangeNode) []*dpb.DescriptorProto_ExtensionRange {
	opts := r.asUninterpretedOptions(node.options.Elements())
	ers := make([]*dpb.DescriptorProto_ExtensionRange, len(node.ranges))
	for i, rng := range node.ranges {
		er := &dpb.DescriptorProto_ExtensionRange{
			Start: proto.Int32(rng.st),
			End:   proto.Int32(rng.en + 1),
		}
		if len(opts) > 0 {
			er.Options = &dpb.ExtensionRangeOptions{UninterpretedOption: opts}
		}
		r.putExtensionRangeNode(er, rng)
		ers[i] = er
	}
	return ers
}

func (r *parseResult) asEnumValue(ev *enumValueNode) *dpb.EnumValueDescriptorProto {
	num := int32(ev.number.val)
	evd := &dpb.EnumValueDescriptorProto{Name: proto.String(ev.name.val), Number: proto.Int32(num)}
	r.putEnumValueNode(evd, ev)
	if opts := ev.options.Elements(); len(opts) > 0 {
		evd.Options = &dpb.EnumValueOptions{UninterpretedOption: r.asUninterpretedOptions(opts)}
	}
	return evd
}

func (r *parseResult) asMethodDescriptor(node *methodNode) *dpb.MethodDescriptorProto {
	md := &dpb.MethodDescriptorProto{
		Name:       proto.String(node.name.val),
		InputType:  proto.String(node.input.msgType.val),
		OutputType: proto.String(node.output.msgType.val),
	}
	r.putMethodNode(md, node)
	if node.input.streamKeyword != nil {
		md.ClientStreaming = proto.Bool(true)
	}
	if node.output.streamKeyword != nil {
		md.ServerStreaming = proto.Bool(true)
	}
	// protoc always adds a MethodOptions if there are brackets
	// We have a non-nil node.options if there are brackets
	// We do the same to match protoc as closely as possible
	// https://github.com/protocolbuffers/protobuf/blob/0c3f43a6190b77f1f68b7425d1b7e1a8257a8d0c/src/google/protobuf/compiler/parser.cc#L2152
	if node.options != nil {
		md.Options = &dpb.MethodOptions{UninterpretedOption: r.asUninterpretedOptions(node.options)}
	}
	return md
}

func (r *parseResult) asEnumDescriptor(en *enumNode) *dpb.EnumDescriptorProto {
	ed := &dpb.EnumDescriptorProto{Name: proto.String(en.name.val)}
	r.putEnumNode(ed, en)
	for _, decl := range en.decls {
		if decl.option != nil {
			if ed.Options == nil {
				ed.Options = &dpb.EnumOptions{}
			}
			ed.Options.UninterpretedOption = append(ed.Options.UninterpretedOption, r.asUninterpretedOption(decl.option))
		} else if decl.value != nil {
			ed.Value = append(ed.Value, r.asEnumValue(decl.value))
		} else if decl.reserved != nil {
			for _, n := range decl.reserved.names {
				ed.ReservedName = append(ed.ReservedName, n.val)
			}
			for _, rng := range decl.reserved.ranges {
				ed.ReservedRange = append(ed.ReservedRange, r.asEnumReservedRange(rng))
			}
		}
	}
	return ed
}

func (r *parseResult) asEnumReservedRange(rng *rangeNode) *dpb.EnumDescriptorProto_EnumReservedRange {
	rr := &dpb.EnumDescriptorProto_EnumReservedRange{
		Start: proto.Int32(rng.st),
		End:   proto.Int32(rng.en),
	}
	r.putEnumReservedRangeNode(rr, rng)
	return rr
}

func (r *parseResult) asMessageDescriptor(node *messageNode, isProto3 bool) *dpb.DescriptorProto {
	msgd := &dpb.DescriptorProto{Name: proto.String(node.name.val)}
	r.putMessageNode(msgd, node)
	r.addMessageDecls(msgd, node.decls, isProto3)
	return msgd
}

func (r *parseResult) addMessageDecls(msgd *dpb.DescriptorProto, decls []*messageElement, isProto3 bool) {
	for _, decl := range decls {
		if decl.enum != nil {
			msgd.EnumType = append(msgd.EnumType, r.asEnumDescriptor(decl.enum))
		} else if decl.extend != nil {
			r.addExtensions(decl.extend, &msgd.Extension, &msgd.NestedType, isProto3)
		} else if decl.extensionRange != nil {
			msgd.ExtensionRange = append(msgd.ExtensionRange, r.asExtensionRanges(decl.extensionRange)...)
		} else if decl.field != nil {
			msgd.Field = append(msgd.Field, r.asFieldDescriptor(decl.field))
		} else if decl.mapField != nil {
			fd, md := r.asMapDescriptors(decl.mapField, isProto3)
			msgd.Field = append(msgd.Field, fd)
			msgd.NestedType = append(msgd.NestedType, md)
		} else if decl.group != nil {
			fd, md := r.asGroupDescriptors(decl.group, isProto3)
			msgd.Field = append(msgd.Field, fd)
			msgd.NestedType = append(msgd.NestedType, md)
		} else if decl.oneOf != nil {
			oodIndex := len(msgd.OneofDecl)
			ood := &dpb.OneofDescriptorProto{Name: proto.String(decl.oneOf.name.val)}
			r.putOneOfNode(ood, decl.oneOf)
			msgd.OneofDecl = append(msgd.OneofDecl, ood)
			for _, oodecl := range decl.oneOf.decls {
				if oodecl.option != nil {
					if ood.Options == nil {
						ood.Options = &dpb.OneofOptions{}
					}
					ood.Options.UninterpretedOption = append(ood.Options.UninterpretedOption, r.asUninterpretedOption(oodecl.option))
				} else if oodecl.field != nil {
					fd := r.asFieldDescriptor(oodecl.field)
					fd.OneofIndex = proto.Int32(int32(oodIndex))
					msgd.Field = append(msgd.Field, fd)
				} else if oodecl.group != nil {
					fd, md := r.asGroupDescriptors(oodecl.group, isProto3)
					fd.OneofIndex = proto.Int32(int32(oodIndex))
					msgd.Field = append(msgd.Field, fd)
					msgd.NestedType = append(msgd.NestedType, md)
				}
			}
		} else if decl.option != nil {
			if msgd.Options == nil {
				msgd.Options = &dpb.MessageOptions{}
			}
			msgd.Options.UninterpretedOption = append(msgd.Options.UninterpretedOption, r.asUninterpretedOption(decl.option))
		} else if decl.nested != nil {
			msgd.NestedType = append(msgd.NestedType, r.asMessageDescriptor(decl.nested, isProto3))
		} else if decl.reserved != nil {
			for _, n := range decl.reserved.names {
				msgd.ReservedName = append(msgd.ReservedName, n.val)
			}
			for _, rng := range decl.reserved.ranges {
				msgd.ReservedRange = append(msgd.ReservedRange, r.asMessageReservedRange(rng))
			}
		}
	}
}

func (r *parseResult) asMessageReservedRange(rng *rangeNode) *dpb.DescriptorProto_ReservedRange {
	rr := &dpb.DescriptorProto_ReservedRange{
		Start: proto.Int32(rng.st),
		End:   proto.Int32(rng.en + 1),
	}
	r.putMessageReservedRangeNode(rr, rng)
	return rr
}

func (r *parseResult) asServiceDescriptor(svc *serviceNode) *dpb.ServiceDescriptorProto {
	sd := &dpb.ServiceDescriptorProto{Name: proto.String(svc.name.val)}
	r.putServiceNode(sd, svc)
	for _, decl := range svc.decls {
		if decl.option != nil {
			if sd.Options == nil {
				sd.Options = &dpb.ServiceOptions{}
			}
			sd.Options.UninterpretedOption = append(sd.Options.UninterpretedOption, r.asUninterpretedOption(decl.option))
		} else if decl.rpc != nil {
			sd.Method = append(sd.Method, r.asMethodDescriptor(decl.rpc))
		}
	}
	return sd
}

func toNameParts(ident *compoundIdentNode, offset int) []*optionNamePartNode {
	parts := strings.Split(ident.val[offset:], ".")
	ret := make([]*optionNamePartNode, len(parts))
	for i, p := range parts {
		ret[i] = &optionNamePartNode{text: ident, offset: offset, length: len(p)}
		ret[i].setRange(ident, ident)
		offset += len(p) + 1
	}
	return ret
}

func checkUint64InInt32Range(lex protoLexer, pos *SourcePos, v uint64) {
	if v > math.MaxInt32 {
		lexError(lex, pos, fmt.Sprintf("constant %d is out of range for int32 (%d to %d)", v, math.MinInt32, math.MaxInt32))
	}
}

func checkInt64InInt32Range(lex protoLexer, pos *SourcePos, v int64) {
	if v > math.MaxInt32 || v < math.MinInt32 {
		lexError(lex, pos, fmt.Sprintf("constant %d is out of range for int32 (%d to %d)", v, math.MinInt32, math.MaxInt32))
	}
}

func checkTag(lex protoLexer, pos *SourcePos, v uint64) {
	if v < 1 {
		lexError(lex, pos, fmt.Sprintf("tag number %d must be greater than zero", v))
	} else if v > internal.MaxTag {
		lexError(lex, pos, fmt.Sprintf("tag number %d is higher than max allowed tag number (%d)", v, internal.MaxTag))
	} else if v >= internal.SpecialReservedStart && v <= internal.SpecialReservedEnd {
		lexError(lex, pos, fmt.Sprintf("tag number %d is in disallowed reserved range %d-%d", v, internal.SpecialReservedStart, internal.SpecialReservedEnd))
	}
}

func aggToString(agg []*aggregateEntryNode, buf *bytes.Buffer) {
	buf.WriteString("{")
	for _, a := range agg {
		buf.WriteString(" ")
		buf.WriteString(a.name.value())
		if v, ok := a.val.(*aggregateLiteralNode); ok {
			aggToString(v.elements, buf)
		} else {
			buf.WriteString(": ")
			elementToString(a.val.value(), buf)
		}
	}
	buf.WriteString(" }")
}

func elementToString(v interface{}, buf *bytes.Buffer) {
	switch v := v.(type) {
	case bool, int64, uint64, identifier:
		fmt.Fprintf(buf, "%v", v)
	case float64:
		if math.IsInf(v, 1) {
			buf.WriteString(": inf")
		} else if math.IsInf(v, -1) {
			buf.WriteString(": -inf")
		} else if math.IsNaN(v) {
			buf.WriteString(": nan")
		} else {
			fmt.Fprintf(buf, ": %v", v)
		}
	case string:
		buf.WriteRune('"')
		writeEscapedBytes(buf, []byte(v))
		buf.WriteRune('"')
	case []valueNode:
		buf.WriteString(": [")
		first := true
		for _, e := range v {
			if first {
				first = false
			} else {
				buf.WriteString(", ")
			}
			elementToString(e.value(), buf)
		}
		buf.WriteString("]")
	case []*aggregateEntryNode:
		aggToString(v, buf)
	}
}

func writeEscapedBytes(buf *bytes.Buffer, b []byte) {
	for _, c := range b {
		switch c {
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\t':
			buf.WriteString("\\t")
		case '"':
			buf.WriteString("\\\"")
		case '\'':
			buf.WriteString("\\'")
		case '\\':
			buf.WriteString("\\\\")
		default:
			if c >= 0x20 && c <= 0x7f && c != '"' && c != '\\' {
				// simple printable characters
				buf.WriteByte(c)
			} else {
				// use octal escape for all other values
				buf.WriteRune('\\')
				buf.WriteByte('0' + ((c >> 6) & 0x7))
				buf.WriteByte('0' + ((c >> 3) & 0x7))
				buf.WriteByte('0' + (c & 0x7))
			}
		}
	}
}

func basicValidate(res *parseResult) {
	fd := res.fd
	isProto3 := fd.GetSyntax() == "proto3"

	for _, md := range fd.MessageType {
		if validateMessage(res, isProto3, "", md) != nil {
			return
		}
	}

	for _, ed := range fd.EnumType {
		if validateEnum(res, isProto3, "", ed) != nil {
			return
		}
	}

	for _, fld := range fd.Extension {
		if validateField(res, isProto3, "", fld) != nil {
			return
		}
	}
}

func validateMessage(res *parseResult, isProto3 bool, prefix string, md *dpb.DescriptorProto) error {
	nextPrefix := md.GetName() + "."

	for _, fld := range md.Field {
		if err := validateField(res, isProto3, nextPrefix, fld); err != nil {
			return err
		}
	}
	for _, fld := range md.Extension {
		if err := validateField(res, isProto3, nextPrefix, fld); err != nil {
			return err
		}
	}
	for _, ed := range md.EnumType {
		if err := validateEnum(res, isProto3, nextPrefix, ed); err != nil {
			return err
		}
	}
	for _, nmd := range md.NestedType {
		if err := validateMessage(res, isProto3, nextPrefix, nmd); err != nil {
			return err
		}
	}

	scope := fmt.Sprintf("message %s%s", prefix, md.GetName())

	if isProto3 && len(md.ExtensionRange) > 0 {
		n := res.getExtensionRangeNode(md.ExtensionRange[0])
		if err := res.errs.handleError(ErrorWithSourcePos{Pos: n.start(), Underlying: fmt.Errorf("%s: extension ranges are not allowed in proto3", scope)}); err != nil {
			return err
		}
	}

	if index, err := findOption(res, scope, md.Options.GetUninterpretedOption(), "map_entry"); err != nil {
		if err := res.errs.handleError(err); err != nil {
			return err
		}
	} else if index >= 0 {
		opt := md.Options.UninterpretedOption[index]
		optn := res.getOptionNode(opt)
		md.Options.UninterpretedOption = removeOption(md.Options.UninterpretedOption, index)
		valid := false
		if opt.IdentifierValue != nil {
			if opt.GetIdentifierValue() == "true" {
				valid = true
				if err := res.errs.handleError(ErrorWithSourcePos{Pos: optn.getValue().start(), Underlying: fmt.Errorf("%s: map_entry option should not be set explicitly; use map type instead", scope)}); err != nil {
					return err
				}
			} else if opt.GetIdentifierValue() == "false" {
				valid = true
				md.Options.MapEntry = proto.Bool(false)
			}
		}
		if !valid {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: optn.getValue().start(), Underlying: fmt.Errorf("%s: expecting bool value for map_entry option", scope)}); err != nil {
				return err
			}
		}
	}

	// reserved ranges should not overlap
	rsvd := make(tagRanges, len(md.ReservedRange))
	for i, r := range md.ReservedRange {
		n := res.getMessageReservedRangeNode(r)
		rsvd[i] = tagRange{start: r.GetStart(), end: r.GetEnd(), node: n}

	}
	sort.Sort(rsvd)
	for i := 1; i < len(rsvd); i++ {
		if rsvd[i].start < rsvd[i-1].end {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: rsvd[i].node.start(), Underlying: fmt.Errorf("%s: reserved ranges overlap: %d to %d and %d to %d", scope, rsvd[i-1].start, rsvd[i-1].end-1, rsvd[i].start, rsvd[i].end-1)}); err != nil {
				return err
			}
		}
	}

	// extensions ranges should not overlap
	exts := make(tagRanges, len(md.ExtensionRange))
	for i, r := range md.ExtensionRange {
		n := res.getExtensionRangeNode(r)
		exts[i] = tagRange{start: r.GetStart(), end: r.GetEnd(), node: n}
	}
	sort.Sort(exts)
	for i := 1; i < len(exts); i++ {
		if exts[i].start < exts[i-1].end {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: exts[i].node.start(), Underlying: fmt.Errorf("%s: extension ranges overlap: %d to %d and %d to %d", scope, exts[i-1].start, exts[i-1].end-1, exts[i].start, exts[i].end-1)}); err != nil {
				return err
			}
		}
	}

	// see if any extension range overlaps any reserved range
	var i, j int // i indexes rsvd; j indexes exts
	for i < len(rsvd) && j < len(exts) {
		if rsvd[i].start >= exts[j].start && rsvd[i].start < exts[j].end ||
			exts[j].start >= rsvd[i].start && exts[j].start < rsvd[i].end {

			var pos *SourcePos
			if rsvd[i].start >= exts[j].start && rsvd[i].start < exts[j].end {
				pos = rsvd[i].node.start()
			} else {
				pos = exts[j].node.start()
			}
			// ranges overlap
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: pos, Underlying: fmt.Errorf("%s: extension range %d to %d overlaps reserved range %d to %d", scope, exts[j].start, exts[j].end-1, rsvd[i].start, rsvd[i].end-1)}); err != nil {
				return err
			}
		}
		if rsvd[i].start < exts[j].start {
			i++
		} else {
			j++
		}
	}

	// now, check that fields don't re-use tags and don't try to use extension
	// or reserved ranges or reserved names
	rsvdNames := map[string]struct{}{}
	for _, n := range md.ReservedName {
		rsvdNames[n] = struct{}{}
	}
	fieldTags := map[int32]string{}
	for _, fld := range md.Field {
		fn := res.getFieldNode(fld)
		if _, ok := rsvdNames[fld.GetName()]; ok {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: fn.fieldName().start(), Underlying: fmt.Errorf("%s: field %s is using a reserved name", scope, fld.GetName())}); err != nil {
				return err
			}
		}
		if existing := fieldTags[fld.GetNumber()]; existing != "" {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: fn.fieldTag().start(), Underlying: fmt.Errorf("%s: fields %s and %s both have the same tag %d", scope, existing, fld.GetName(), fld.GetNumber())}); err != nil {
				return err
			}
		}
		fieldTags[fld.GetNumber()] = fld.GetName()
		// check reserved ranges
		r := sort.Search(len(rsvd), func(index int) bool { return rsvd[index].end > fld.GetNumber() })
		if r < len(rsvd) && rsvd[r].start <= fld.GetNumber() {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: fn.fieldTag().start(), Underlying: fmt.Errorf("%s: field %s is using tag %d which is in reserved range %d to %d", scope, fld.GetName(), fld.GetNumber(), rsvd[r].start, rsvd[r].end-1)}); err != nil {
				return err
			}
		}
		// and check extension ranges
		e := sort.Search(len(exts), func(index int) bool { return exts[index].end > fld.GetNumber() })
		if e < len(exts) && exts[e].start <= fld.GetNumber() {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: fn.fieldTag().start(), Underlying: fmt.Errorf("%s: field %s is using tag %d which is in extension range %d to %d", scope, fld.GetName(), fld.GetNumber(), exts[e].start, exts[e].end-1)}); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateEnum(res *parseResult, isProto3 bool, prefix string, ed *dpb.EnumDescriptorProto) error {
	scope := fmt.Sprintf("enum %s%s", prefix, ed.GetName())

	allowAlias := false
	if index, err := findOption(res, scope, ed.Options.GetUninterpretedOption(), "allow_alias"); err != nil {
		if err := res.errs.handleError(err); err != nil {
			return err
		}
	} else if index >= 0 {
		opt := ed.Options.UninterpretedOption[index]
		valid := false
		if opt.IdentifierValue != nil {
			if opt.GetIdentifierValue() == "true" {
				allowAlias = true
				valid = true
			} else if opt.GetIdentifierValue() == "false" {
				valid = true
			}
		}
		if !valid {
			optNode := res.getOptionNode(opt)
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: optNode.getValue().start(), Underlying: fmt.Errorf("%s: expecting bool value for allow_alias option", scope)}); err != nil {
				return err
			}
		}
	}

	if isProto3 && ed.Value[0].GetNumber() != 0 {
		evNode := res.getEnumValueNode(ed.Value[0])
		if err := res.errs.handleError(ErrorWithSourcePos{Pos: evNode.getNumber().start(), Underlying: fmt.Errorf("%s: proto3 requires that first value in enum have numeric value of 0", scope)}); err != nil {
			return err
		}
	}

	if !allowAlias {
		// make sure all value numbers are distinct
		vals := map[int32]string{}
		for _, evd := range ed.Value {
			if existing := vals[evd.GetNumber()]; existing != "" {
				evNode := res.getEnumValueNode(evd)
				if err := res.errs.handleError(ErrorWithSourcePos{Pos: evNode.getNumber().start(), Underlying: fmt.Errorf("%s: values %s and %s both have the same numeric value %d; use allow_alias option if intentional", scope, existing, evd.GetName(), evd.GetNumber())}); err != nil {
					return err
				}
			}
			vals[evd.GetNumber()] = evd.GetName()
		}
	}

	// reserved ranges should not overlap
	rsvd := make(tagRanges, len(ed.ReservedRange))
	for i, r := range ed.ReservedRange {
		n := res.getEnumReservedRangeNode(r)
		rsvd[i] = tagRange{start: r.GetStart(), end: r.GetEnd(), node: n}
	}
	sort.Sort(rsvd)
	for i := 1; i < len(rsvd); i++ {
		if rsvd[i].start <= rsvd[i-1].end {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: rsvd[i].node.start(), Underlying: fmt.Errorf("%s: reserved ranges overlap: %d to %d and %d to %d", scope, rsvd[i-1].start, rsvd[i-1].end, rsvd[i].start, rsvd[i].end)}); err != nil {
				return err
			}
		}
	}

	// now, check that fields don't re-use tags and don't try to use extension
	// or reserved ranges or reserved names
	rsvdNames := map[string]struct{}{}
	for _, n := range ed.ReservedName {
		rsvdNames[n] = struct{}{}
	}
	for _, ev := range ed.Value {
		evn := res.getEnumValueNode(ev)
		if _, ok := rsvdNames[ev.GetName()]; ok {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: evn.getName().start(), Underlying: fmt.Errorf("%s: value %s is using a reserved name", scope, ev.GetName())}); err != nil {
				return err
			}
		}
		// check reserved ranges
		r := sort.Search(len(rsvd), func(index int) bool { return rsvd[index].end >= ev.GetNumber() })
		if r < len(rsvd) && rsvd[r].start <= ev.GetNumber() {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: evn.getNumber().start(), Underlying: fmt.Errorf("%s: value %s is using number %d which is in reserved range %d to %d", scope, ev.GetName(), ev.GetNumber(), rsvd[r].start, rsvd[r].end)}); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateField(res *parseResult, isProto3 bool, prefix string, fld *dpb.FieldDescriptorProto) error {
	scope := fmt.Sprintf("field %s%s", prefix, fld.GetName())

	node := res.getFieldNode(fld)
	if isProto3 {
		if fld.GetType() == dpb.FieldDescriptorProto_TYPE_GROUP {
			n := node.(*groupNode)
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: n.groupKeyword.start(), Underlying: fmt.Errorf("%s: groups are not allowed in proto3", scope)}); err != nil {
				return err
			}
		} else if fld.Label != nil && fld.GetLabel() != dpb.FieldDescriptorProto_LABEL_REPEATED {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: node.fieldLabel().start(), Underlying: fmt.Errorf("%s: field has label %v, but proto3 must omit labels other than 'repeated'", scope, fld.GetLabel())}); err != nil {
				return err
			}
		}
		if index, err := findOption(res, scope, fld.Options.GetUninterpretedOption(), "default"); err != nil {
			if err := res.errs.handleError(err); err != nil {
				return err
			}
		} else if index >= 0 {
			optNode := res.getOptionNode(fld.Options.GetUninterpretedOption()[index])
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: optNode.getName().start(), Underlying: fmt.Errorf("%s: default values are not allowed in proto3", scope)}); err != nil {
				return err
			}
		}
	} else {
		if fld.Label == nil && fld.OneofIndex == nil {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: node.fieldName().start(), Underlying: fmt.Errorf("%s: field has no label, but proto2 must indicate 'optional' or 'required'", scope)}); err != nil {
				return err
			}
		}
		if fld.GetExtendee() != "" && fld.Label != nil && fld.GetLabel() == dpb.FieldDescriptorProto_LABEL_REQUIRED {
			if err := res.errs.handleError(ErrorWithSourcePos{Pos: node.fieldLabel().start(), Underlying: fmt.Errorf("%s: extension fields cannot be 'required'", scope)}); err != nil {
				return err
			}
		}
	}

	// finally, set any missing label to optional
	if fld.Label == nil {
		fld.Label = dpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	}

	return nil
}

func findOption(res *parseResult, scope string, opts []*dpb.UninterpretedOption, name string) (int, error) {
	found := -1
	for i, opt := range opts {
		if len(opt.Name) != 1 {
			continue
		}
		if opt.Name[0].GetIsExtension() || opt.Name[0].GetNamePart() != name {
			continue
		}
		if found >= 0 {
			optNode := res.getOptionNode(opt)
			return -1, ErrorWithSourcePos{Pos: optNode.getName().start(), Underlying: fmt.Errorf("%s: option %s cannot be defined more than once", scope, name)}
		}
		found = i
	}
	return found, nil
}

func removeOption(uo []*dpb.UninterpretedOption, indexToRemove int) []*dpb.UninterpretedOption {
	if indexToRemove == 0 {
		return uo[1:]
	} else if int(indexToRemove) == len(uo)-1 {
		return uo[:len(uo)-1]
	} else {
		return append(uo[:indexToRemove], uo[indexToRemove+1:]...)
	}
}

type tagRange struct {
	start int32
	end   int32
	node  rangeDecl
}

type tagRanges []tagRange

func (r tagRanges) Len() int {
	return len(r)
}

func (r tagRanges) Less(i, j int) bool {
	return r[i].start < r[j].start ||
		(r[i].start == r[j].start && r[i].end < r[j].end)
}

func (r tagRanges) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
