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
        }
      ],
      topics: []
    }
  },
  mounted() {
    const topicClient = new TopicServiceClient('http://localhost:8081')

    const request = new ListTopicsRequest()

    topicClient.listTopics(request, {}, (err, response) => {
      console.log(err)
      console.log(err, response.toObject().topicsList)
      this.topics = response.toObject().topicsList
    })
  }
}
</script>
