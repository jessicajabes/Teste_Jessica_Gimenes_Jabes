import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import Detalhes from '../views/Detalhes.vue'

const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/operadora/:cnpj',
    name: 'Detalhes',
    component: Detalhes,
    props: true
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
