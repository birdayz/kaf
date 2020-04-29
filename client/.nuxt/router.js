import Vue from 'vue'
import Router from 'vue-router'
import { interopDefault } from './utils'
import scrollBehavior from './router.scrollBehavior.js'

const _4f74341e = () => interopDefault(import('../pages/topics.vue' /* webpackChunkName: "pages/topics" */))

// TODO: remove in Nuxt 3
const emptyFn = () => {}
const originalPush = Router.prototype.push
Router.prototype.push = function push (location, onComplete = emptyFn, onAbort) {
  return originalPush.call(this, location, onComplete, onAbort)
}

Vue.use(Router)

export const routerOptions = {
  mode: 'history',
  base: decodeURI('/'),
  linkActiveClass: 'nuxt-link-active',
  linkExactActiveClass: 'nuxt-link-exact-active',
  scrollBehavior,

  routes: [{
    path: "/topics",
    component: _4f74341e,
    name: "topics"
  }, {
    path: "/",
    component: _4f74341e,
    name: "topics"
  }],

  fallback: false
}

export function createRouter () {
  return new Router(routerOptions)
}
