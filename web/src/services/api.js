import axios from 'axios';
// MELHORIA: baseURL lida da variável de ambiente VITE_API_URL (definida em web/.env)
// Fallback para localhost no desenvolvimento local
const api = axios.create({ baseURL: import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000/api' });
export default api;