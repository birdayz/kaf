/**
 * @fileoverview gRPC-Web generated client stub for kaf.api
 * @enhanceable
 * @public
 */

// GENERATED CODE -- DO NOT EDIT!



const grpc = {};
grpc.web = require('grpc-web');

const proto = {};
proto.kaf = {};
proto.kaf.api = require('./cluster_pb.js');

/**
 * @param {string} hostname
 * @param {?Object} credentials
 * @param {?Object} options
 * @constructor
 * @struct
 * @final
 */
proto.kaf.api.ClusterServiceClient =
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
proto.kaf.api.ClusterServicePromiseClient =
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
 *   !proto.kaf.api.ListClustersRequest,
 *   !proto.kaf.api.ListClustersResponse>}
 */
const methodDescriptor_ClusterService_ListClusters = new grpc.web.MethodDescriptor(
  '/kaf.api.ClusterService/ListClusters',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.ListClustersRequest,
  proto.kaf.api.ListClustersResponse,
  /**
   * @param {!proto.kaf.api.ListClustersRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.ListClustersResponse.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.ListClustersRequest,
 *   !proto.kaf.api.ListClustersResponse>}
 */
const methodInfo_ClusterService_ListClusters = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.ListClustersResponse,
  /**
   * @param {!proto.kaf.api.ListClustersRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.ListClustersResponse.deserializeBinary
);


/**
 * @param {!proto.kaf.api.ListClustersRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.ListClustersResponse)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.ListClustersResponse>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.ClusterServiceClient.prototype.listClusters =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.ClusterService/ListClusters',
      request,
      metadata || {},
      methodDescriptor_ClusterService_ListClusters,
      callback);
};


/**
 * @param {!proto.kaf.api.ListClustersRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.ListClustersResponse>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.ClusterServicePromiseClient.prototype.listClusters =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.ClusterService/ListClusters',
      request,
      metadata || {},
      methodDescriptor_ClusterService_ListClusters);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kaf.api.ConnectClusterRequest,
 *   !proto.kaf.api.ConnectClusterResponse>}
 */
const methodDescriptor_ClusterService_ConnectCluster = new grpc.web.MethodDescriptor(
  '/kaf.api.ClusterService/ConnectCluster',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.ConnectClusterRequest,
  proto.kaf.api.ConnectClusterResponse,
  /**
   * @param {!proto.kaf.api.ConnectClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.ConnectClusterResponse.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.ConnectClusterRequest,
 *   !proto.kaf.api.ConnectClusterResponse>}
 */
const methodInfo_ClusterService_ConnectCluster = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.ConnectClusterResponse,
  /**
   * @param {!proto.kaf.api.ConnectClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.ConnectClusterResponse.deserializeBinary
);


/**
 * @param {!proto.kaf.api.ConnectClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.ConnectClusterResponse)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.ConnectClusterResponse>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.ClusterServiceClient.prototype.connectCluster =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.ClusterService/ConnectCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_ConnectCluster,
      callback);
};


/**
 * @param {!proto.kaf.api.ConnectClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.ConnectClusterResponse>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.ClusterServicePromiseClient.prototype.connectCluster =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.ClusterService/ConnectCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_ConnectCluster);
};


module.exports = proto.kaf.api;

