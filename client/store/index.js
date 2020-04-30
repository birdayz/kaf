import { ClusterServiceClient } from '../../api/cluster_grpc_web_pb.js'
import {
  ListClustersRequest,
  ConnectClusterRequest
} from '../../api/cluster_pb.js'

export const state = () => ({
  currentCluster: null,
  clusters: []
})

export const mutations = {
  setClusters(state, clusters) {
    state.clusters = clusters
    // automatically select a tenant
    if (!state.currentCluster) {
      if (window.localStorage.getItem('clusterName')) {
        state.currentCluster = window.localStorage.getItem('clusterName')
      } else {
        state.currentCluster = clusters[0].name
      }
    }
  },
  setCurrentCluster(state, clusterName) {
    state.currentCluster = clusterName
    window.localStorage.setItem('clusterName', clusterName)

    // Call connect
    console.log('conn')
    const client = new ClusterServiceClient('http://localhost:8081')
    const req = new ConnectClusterRequest()
    req.setName(clusterName)
    client.connectCluster(req, {}, (err, response) => {
      console.log('con', err, response)
    })
  }
}

export const actions = {
  fetchClusters(context) {
    const client = new ClusterServiceClient('http://localhost:8081')
    const req = new ListClustersRequest()
    client.listClusters(req, {}, (err, response) => {
      console.log(err, response)
      const clusters = response.toObject().clustersList
      context.commit('setClusters', clusters)
    })
  }
}
