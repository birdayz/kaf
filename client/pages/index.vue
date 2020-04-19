<template>
  <div>abc</div>
</template>
<script>
import { TopicServiceClient } from '../../api/topic_grpc_web_pb.js'
import { ListTopicsRequest } from '../../api/topic_pb.js'

export default {
  mounted() {
    const topicClient = new TopicServiceClient('http://localhost:8081')

    const request = new ListTopicsRequest()

    topicClient.listTopics(request, {}, function(err, response) {
      // ...
      console.log(err, response)
    })
  }
}
</script>
