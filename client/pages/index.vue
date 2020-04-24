<template>
  <div>
    <v-data-table
      :headers="headers"
      :items="topics"
      :items-per-page="15"
      class="elevation-1"
    ></v-data-table>
  </div>
</template>
<script>
import { TopicServiceClient } from '../../api/topic_grpc_web_pb.js'
import { ListTopicsRequest } from '../../api/topic_pb.js'

export default {
  data() {
    return {
      headers: [
        {
          text: 'Name',
          align: 'start',
          sortable: true,
          value: 'name'
        },
        {
          text: 'Partitions',
          align: 'start',
          sortable: true,
          value: 'numpartitions'
        },
        {
          text: 'Replicas',
          align: 'start',
          sortable: true,
          value: 'numreplicas'
        }
      ],
      topics: []
    }
  },
  computed: {
    currentCluster: {
      get() {
        return this.$store.state.currentCluster
      }
    }
  },
  watch: {
    currentCluster: {
      immediate: true,
      handler(newVal, oldVal) {
        console.log('watch', newVal, oldVal)
        if (newVal && newVal !== oldVal) {
          const topicClient = new TopicServiceClient('http://localhost:8081')

          const request = new ListTopicsRequest()
          request.setCluster(this.currentCluster)

          topicClient.listTopics(request, {}, (err, response) => {
            console.log(err, response.toObject().topicsList)
            this.topics = response.toObject().topicsList
          })
        }
      }
    }
  },
  mounted() {}
}
</script>
