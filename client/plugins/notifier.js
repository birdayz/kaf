export default ({ app, store }, inject) => {
  inject('notifier', {
    showMessage({ content = '', color = '', connected = null }) {
      store.commit('snackbar/showMessage', { content, color, connected })
    }
  })
}
