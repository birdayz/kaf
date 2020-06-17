<template>
  <div>
    <v-data-table
      :headers="headers"
      :items="topics"
      :items-per-page="15"
      class="elevation-1"
    >
      <template v-slot:item.totalHighWatermarks="{ item }">
        <span>{{ numer(item.totalHighWatermarks).format('0 a') }}</span>
      </template>
      <template v-slot:item.logDirBytes="{ item }">
        <span>{{ pb(item.logDirBytes) }}</span>
      </template>
    </v-data-table>
    <v-snackbar v-model="snackbar">
      {{ text }}
      <v-btn color="pink" text @click="snackbar = false">Close</v-btn>
    </v-snackbar>
  </div>
</template>
<script>
import numeral from 'numeral'
import prettyBytes from 'pretty-bytes'
import { TopicServiceClient } from '../../api/topic_grpc_web_pb.js'
import {
  ListTopicsRequest
  // GetHighWatermarksRequest
} from '../../api/topic_pb.js'

export default {
  data() {
    return {
      snackbar: false,
      text: "Hello, I'm a snackbar",
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
          value: 'numPartitions'
        },
        {
          text: 'Replicas',
          align: 'start',
          sortable: true,
          value: 'numReplicas'
        },
        {
          text: 'Messages',
          align: 'start',
          sortable: true,
          value: 'totalHighWatermarks'
        },
        {
          text: 'Size',
          align: 'start',
          sortable: true,
          value: 'logDirBytes'
        }
      ],
      topics: [],
      messages: 0
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
        if (newVal && newVal !== oldVal) {
          this.topics = []
          const topicClient = new TopicServiceClient('http://localhost:8081')

          const request = new ListTopicsRequest()
          request.setCluster(this.currentCluster)

          topicClient.listTopics(request, {}, (err, response) => {
            if (err) {
              console.log('err', err)
              this.$notifier.showMessage({
                content:
                  'Failed to connect to cluster ' +
                  newVal +
                  ' :' +
                  JSON.stringify(err),
                color: 'error',
                connected: false
              })
            } else {
              console.log('xx', response.toObject())
              this.topics = response.toObject().topicsList

              this.$notifier.showMessage({
                content: 'Connected to cluster ' + newVal,
                connected: true,
                color: 'info'
              })
            }
          })
        }
      }
    }
  },
  mounted() {},
  methods: {
    numer: numeral,
    pb: prettyBytes
  }
}
</script>
