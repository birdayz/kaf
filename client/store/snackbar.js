export const state = () => ({
  content: '',
  color: '',
  connected: null
})

export const mutations = {
  showMessage(state, payload) {
    state.content = payload.content
    state.color = payload.color
    state.connected = payload.connected
  }
}
