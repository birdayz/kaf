export default ({ app, store }, inject) => {
  inject('notifier', {
    showMessage({ content = '', color = '' }) {
      store.commit('snackbar/showMessage', { content, color })
    }
  })
}
