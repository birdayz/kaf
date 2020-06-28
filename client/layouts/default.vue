<template>
  <v-app id="inspire">
    <v-navigation-drawer v-model="drawer" app clipped>
      <v-list dense>
        <v-list-item link to="/topics">
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
        style="width: 100px; max-width: 200px; padding-left: 15px; padding-top: 30px;"
        label="Cluster"
        no-data-text="Loading.."
        placeholder="Loading.."
        autowidth
        flat
      ></v-select>
    </v-app-bar>

    <v-content>
      <v-container fluid>
        <v-breadcrumbs :items="crumbs">
          <template v-slot:divider>
            <v-icon>mdi-chevron-right</v-icon>
          </template>
        </v-breadcrumbs>
        <nuxt />
      </v-container>
    </v-content>

    <v-footer app padless>
      <v-row no-gutters>
        <div class="body-2" style="padding: 5px;">
          <v-icon v-if="showOn">mdi-earth</v-icon>
          <v-icon v-if="showOff">mdi-earth-off</v-icon>
          | {{ message }}
        </div>
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
    drawer: true,
    message: 'Ready',
    showOn: false,
    showOff: false
  }),
  computed: {
    crumbs() {
      const crumbs = []
      this.$route.matched.map((item, i, { length }) => {
        const crumb = {}
        crumb.to = item.path
        crumb.text = item.name.toUpperCase()

        crumbs.push(crumb)
      })

      return crumbs
    },
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
    this.$store.subscribe((mutation, state) => {
      if (mutation.type === 'snackbar/showMessage') {
        console.log('got msg', state.snackbar)
        this.message = state.snackbar.content
        if (state.snackbar.connected != null) {
          if (state.snackbar.connected === true) {
            this.showOn = true
            this.showOff = false
          } else {
            this.showOn = false
            this.showOff = true
          }
        } else {
          this.showOn = false
          this.showOff = false
        }
        // this.color = state.snackbar.color
        // this.show = true
      }
    })
  }
}
</script>
