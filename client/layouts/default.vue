<template>
  <v-app id="inspire">
    <v-navigation-drawer v-model="drawer" app clipped>
      <v-list dense>
        <v-list-item link>
          <v-list-item-action>
            <v-icon>mdi-message-text</v-icon>
          </v-list-item-action>
          <v-list-item-content>
            <v-list-item-title>Topics</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
        <v-list-item link>
          <v-list-item-action>
            <v-icon>mdi-cog</v-icon>
          </v-list-item-action>
          <v-list-item-content>
            <v-list-item-title>Settings</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
      </v-list>
    </v-navigation-drawer>

    <v-app-bar app clipped-left>
      <img src="kafka.png" width="40px" @click.stop="drawer = !drawer" />
      <v-toolbar-title>kaf</v-toolbar-title>
      <v-select
        v-model="currentCluster"
        :items="clusters"
        item-text="name"
        style="width: 100px; max-width: 200px; padding-left: 15px; padding-top: 25px;"
        label="Cluster"
        no-data-text="Loading.."
        placeholder="Loading.."
        autowidth
        flat
      ></v-select>
    </v-app-bar>

    <v-content>
      <v-container fluid>
        <nuxt />
      </v-container>
    </v-content>

    <v-footer app padless>
      <v-row justify="center" no-gutters>
        <v-btn color="white" text href="https://github.com/birdayz/kaf"
          >GitHub</v-btn
        >
      </v-row>
    </v-footer>
  </v-app>
</template>

<script>
export default {
  props: {
    source: String
  },
  data: () => ({
    drawer: true
  }),
  computed: {
    clusters() {
      return this.$store.state.clusters
    },
    currentCluster: {
      get() {
        return this.$store.state.currentCluster
      },
      set(value) {
        this.$store.commit('setCurrentCluster', value)
      }
    }
  },
  mounted() {
    this.$store.dispatch('fetchClusters')
  },
  created() {
    this.$vuetify.theme.dark = true
  }
}
</script>
