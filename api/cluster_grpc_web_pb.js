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
 *   !proto.kaf.api.CreateClusterRequest,
 *   !proto.kaf.api.Cluster>}
 */
const methodDescriptor_ClusterService_CreateCluster = new grpc.web.MethodDescriptor(
  '/kaf.api.ClusterService/CreateCluster',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.CreateClusterRequest,
  proto.kaf.api.Cluster,
  /**
   * @param {!proto.kaf.api.CreateClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Cluster.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.CreateClusterRequest,
 *   !proto.kaf.api.Cluster>}
 */
const methodInfo_ClusterService_CreateCluster = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.Cluster,
  /**
   * @param {!proto.kaf.api.CreateClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Cluster.deserializeBinary
);


/**
 * @param {!proto.kaf.api.CreateClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.Cluster)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.Cluster>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.ClusterServiceClient.prototype.createCluster =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.ClusterService/CreateCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_CreateCluster,
      callback);
};


/**
 * @param {!proto.kaf.api.CreateClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.Cluster>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.ClusterServicePromiseClient.prototype.createCluster =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.ClusterService/CreateCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_CreateCluster);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kaf.api.GetClusterRequest,
 *   !proto.kaf.api.Cluster>}
 */
const methodDescriptor_ClusterService_GetCluster = new grpc.web.MethodDescriptor(
  '/kaf.api.ClusterService/GetCluster',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.GetClusterRequest,
  proto.kaf.api.Cluster,
  /**
   * @param {!proto.kaf.api.GetClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Cluster.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.GetClusterRequest,
 *   !proto.kaf.api.Cluster>}
 */
const methodInfo_ClusterService_GetCluster = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.Cluster,
  /**
   * @param {!proto.kaf.api.GetClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Cluster.deserializeBinary
);


/**
 * @param {!proto.kaf.api.GetClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.Cluster)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.Cluster>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.ClusterServiceClient.prototype.getCluster =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.ClusterService/GetCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_GetCluster,
      callback);
};


/**
 * @param {!proto.kaf.api.GetClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.Cluster>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.ClusterServicePromiseClient.prototype.getCluster =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.ClusterService/GetCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_GetCluster);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.kaf.api.UpdateClusterRequest,
 *   !proto.kaf.api.Cluster>}
 */
const methodDescriptor_ClusterService_UpdateCluster = new grpc.web.MethodDescriptor(
  '/kaf.api.ClusterService/UpdateCluster',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.UpdateClusterRequest,
  proto.kaf.api.Cluster,
  /**
   * @param {!proto.kaf.api.UpdateClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Cluster.deserializeBinary
);


/**
 * @const
 * @type {!grpc.web.AbstractClientBase.MethodInfo<
 *   !proto.kaf.api.UpdateClusterRequest,
 *   !proto.kaf.api.Cluster>}
 */
const methodInfo_ClusterService_UpdateCluster = new grpc.web.AbstractClientBase.MethodInfo(
  proto.kaf.api.Cluster,
  /**
   * @param {!proto.kaf.api.UpdateClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.kaf.api.Cluster.deserializeBinary
);


/**
 * @param {!proto.kaf.api.UpdateClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.kaf.api.Cluster)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.kaf.api.Cluster>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.ClusterServiceClient.prototype.updateCluster =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.ClusterService/UpdateCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_UpdateCluster,
      callback);
};


/**
 * @param {!proto.kaf.api.UpdateClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.kaf.api.Cluster>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.ClusterServicePromiseClient.prototype.updateCluster =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.ClusterService/UpdateCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_UpdateCluster);
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
 *   !proto.kaf.api.DeleteClusterRequest,
 *   !proto.google.protobuf.Empty>}
 */
const methodDescriptor_ClusterService_DeleteCluster = new grpc.web.MethodDescriptor(
  '/kaf.api.ClusterService/DeleteCluster',
  grpc.web.MethodType.UNARY,
  proto.kaf.api.DeleteClusterRequest,
  google_protobuf_empty_pb.Empty,
  /**
   * @param {!proto.kaf.api.DeleteClusterRequest} request
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
 *   !proto.kaf.api.DeleteClusterRequest,
 *   !proto.google.protobuf.Empty>}
 */
const methodInfo_ClusterService_DeleteCluster = new grpc.web.AbstractClientBase.MethodInfo(
  google_protobuf_empty_pb.Empty,
  /**
   * @param {!proto.kaf.api.DeleteClusterRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  google_protobuf_empty_pb.Empty.deserializeBinary
);


/**
 * @param {!proto.kaf.api.DeleteClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.Error, ?proto.google.protobuf.Empty)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.google.protobuf.Empty>|undefined}
 *     The XHR Node Readable Stream
 */
proto.kaf.api.ClusterServiceClient.prototype.deleteCluster =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/kaf.api.ClusterService/DeleteCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_DeleteCluster,
      callback);
};


/**
 * @param {!proto.kaf.api.DeleteClusterRequest} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.google.protobuf.Empty>}
 *     A native promise that resolves to the response
 */
proto.kaf.api.ClusterServicePromiseClient.prototype.deleteCluster =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/kaf.api.ClusterService/DeleteCluster',
      request,
      metadata || {},
      methodDescriptor_ClusterService_DeleteCluster);
};


module.exports = proto.kaf.api;

