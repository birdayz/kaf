/**
 * @fileoverview gRPC-Web generated client stub for kaf.api
 * @enhanceable
 * @public
 */

// GENERATED CODE -- DO NOT EDIT!



const grpc = {};
grpc.web = require('grpc-web');


var google_protobuf_empty_pb = require('google-protobuf/google/protobuf/empty_pb.js')
const proto = {};
proto.kaf = {};
proto.kaf.api = require('./topic_pb.js');

/**
 * @param {string} hostname
 * @param {?Object} credentials
 * @param {?Object} options
 * @constructor
 * @struct
 * @final
 */
proto.kaf.api.TopicServiceClient =
    function(hostname, credentials, options) {
  if (!options) options = {};
  options['format'] = 'text';

  /**
   * @private @const {!grpc.web.GrpcWebClientBase} The client
   */
  this.client_ = new grpc.web.GrpcWebClientBase(options);

  /**
   * @private @const {string} The hostname
   */
  this.hostname_ = hostname;

};


/**
 * @param {string} hostname
 * @param {?Object} credentials
 * @param {?Object} options
 * @constructor
 * @struct
 * @final
 */
proto.kaf.api.TopicServicePromiseClient =
    function(hostname, credentials, options) {
  if (!options) options = {};
  options['format'] = 'text';

  /**
   * @private @const {!grpc.web.GrpcWebClientBase} The client
   */
  this.client_ = new grpc.web.GrpcWebClientBase(options);

  /**
   * @private @const {string} The hostname
   */
  this.hostname_ = hostname;

};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kaf.api.CreateTopicRequest,
 *   !proto.kaf.api.Topic>}
 */
const methodDescriptor_TopicService_CreateTopic = new grpc.web.MethodDescriptor(
  '/kaf.api.TopicService/CreateTopic',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.CreateTopicRequest,
  proto.kaf.api.Topic,
  /**
   * @param {!proto.kaf.api.CreateTopicRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Topic.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.CreateTopicRequest,
 *   !proto.kaf.api.Topic>}
 */
const methodInfo_TopicService_CreateTopic = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.Topic,
  /**
   * @param {!proto.kaf.api.CreateTopicRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Topic.deserializeBinary
);


/**
 * @param {!proto.kaf.api.CreateTopicRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.Topic)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.Topic>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.TopicServiceClient.prototype.createTopic =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.TopicService/CreateTopic',
      request,
      metadata || {},
      methodDescriptor_TopicService_CreateTopic,
      callback);
};


/**
 * @param {!proto.kaf.api.CreateTopicRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.Topic>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.TopicServicePromiseClient.prototype.createTopic =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.TopicService/CreateTopic',
      request,
      metadata || {},
      methodDescriptor_TopicService_CreateTopic);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kaf.api.GetTopicRequest,
 *   !proto.kaf.api.Topic>}
 */
const methodDescriptor_TopicService_GetTopic = new grpc.web.MethodDescriptor(
  '/kaf.api.TopicService/GetTopic',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.GetTopicRequest,
  proto.kaf.api.Topic,
  /**
   * @param {!proto.kaf.api.GetTopicRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Topic.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.GetTopicRequest,
 *   !proto.kaf.api.Topic>}
 */
const methodInfo_TopicService_GetTopic = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.Topic,
  /**
   * @param {!proto.kaf.api.GetTopicRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Topic.deserializeBinary
);


/**
 * @param {!proto.kaf.api.GetTopicRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.Topic)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.Topic>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.TopicServiceClient.prototype.getTopic =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.TopicService/GetTopic',
      request,
      metadata || {},
      methodDescriptor_TopicService_GetTopic,
      callback);
};


/**
 * @param {!proto.kaf.api.GetTopicRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.Topic>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.TopicServicePromiseClient.prototype.getTopic =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.TopicService/GetTopic',
      request,
      metadata || {},
      methodDescriptor_TopicService_GetTopic);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kaf.api.UpdateTopicRequest,
 *   !proto.kaf.api.Topic>}
 */
const methodDescriptor_TopicService_UpdateTopic = new grpc.web.MethodDescriptor(
  '/kaf.api.TopicService/UpdateTopic',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.UpdateTopicRequest,
  proto.kaf.api.Topic,
  /**
   * @param {!proto.kaf.api.UpdateTopicRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Topic.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.UpdateTopicRequest,
 *   !proto.kaf.api.Topic>}
 */
const methodInfo_TopicService_UpdateTopic = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.Topic,
  /**
   * @param {!proto.kaf.api.UpdateTopicRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Topic.deserializeBinary
);


/**
 * @param {!proto.kaf.api.UpdateTopicRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.Topic)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.Topic>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.TopicServiceClient.prototype.updateTopic =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.TopicService/UpdateTopic',
      request,
      metadata || {},
      methodDescriptor_TopicService_UpdateTopic,
      callback);
};


/**
 * @param {!proto.kaf.api.UpdateTopicRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.Topic>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.TopicServicePromiseClient.prototype.updateTopic =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.TopicService/UpdateTopic',
      request,
      metadata || {},
      methodDescriptor_TopicService_UpdateTopic);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kaf.api.ListTopicsRequest,
 *   !proto.kaf.api.ListTopicsResponse>}
 */
const methodDescriptor_TopicService_ListTopics = new grpc.web.MethodDescriptor(
  '/kaf.api.TopicService/ListTopics',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.ListTopicsRequest,
  proto.kaf.api.ListTopicsResponse,
  /**
   * @param {!proto.kaf.api.ListTopicsRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.ListTopicsResponse.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.ListTopicsRequest,
 *   !proto.kaf.api.ListTopicsResponse>}
 */
const methodInfo_TopicService_ListTopics = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.ListTopicsResponse,
  /**
   * @param {!proto.kaf.api.ListTopicsRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.ListTopicsResponse.deserializeBinary
);


/**
 * @param {!proto.kaf.api.ListTopicsRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.ListTopicsResponse)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.ListTopicsResponse>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.TopicServiceClient.prototype.listTopics =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.TopicService/ListTopics',
      request,
      metadata || {},
      methodDescriptor_TopicService_ListTopics,
      callback);
};


/**
 * @param {!proto.kaf.api.ListTopicsRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.ListTopicsResponse>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.TopicServicePromiseClient.prototype.listTopics =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.TopicService/ListTopics',
      request,
      metadata || {},
      methodDescriptor_TopicService_ListTopics);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kaf.api.DeleteTopicRequest,
 *   !proto.google.protobuf.Empty>}
 */
const methodDescriptor_TopicService_DeleteTopic = new grpc.web.MethodDescriptor(
  '/kaf.api.TopicService/DeleteTopic',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.DeleteTopicRequest,
  google_protobuf_empty_pb.Empty,
  /**
   * @param {!proto.kaf.api.DeleteTopicRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  google_protobuf_empty_pb.Empty.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.DeleteTopicRequest,
 *   !proto.google.protobuf.Empty>}
 */
const methodInfo_TopicService_DeleteTopic = new grpc.web.AbstractClientBase.MethodInfo(
  google_protobuf_empty_pb.Empty,
  /**
   * @param {!proto.kaf.api.DeleteTopicRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  google_protobuf_empty_pb.Empty.deserializeBinary
);


/**
 * @param {!proto.kaf.api.DeleteTopicRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.google.protobuf.Empty)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.google.protobuf.Empty>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.TopicServiceClient.prototype.deleteTopic =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.TopicService/DeleteTopic',
      request,
      metadata || {},
      methodDescriptor_TopicService_DeleteTopic,
      callback);
};


/**
 * @param {!proto.kaf.api.DeleteTopicRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.google.protobuf.Empty>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.TopicServicePromiseClient.prototype.deleteTopic =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.TopicService/DeleteTopic',
      request,
      metadata || {},
      methodDescriptor_TopicService_DeleteTopic);
};


module.exports = proto.kaf.api;

