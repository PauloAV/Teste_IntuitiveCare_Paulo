import { createApp } from 'vue'
import './style.css' // Se der erro de novo, apaga essa linha
import App from './App.vue'
import router from './router'

createApp(App).use(router).mount('#app')