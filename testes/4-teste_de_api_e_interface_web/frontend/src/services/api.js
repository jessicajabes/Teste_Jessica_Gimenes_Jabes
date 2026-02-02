import axios from 'axios'

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:8000',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// Interceptor para tratamento de erros
api.interceptors.response.use(
  response => response,
  error => {
    if (error.code === 'ECONNABORTED') {
      return Promise.reject(new Error('Timeout: O servidor demorou muito para responder'))
    }
    
    if (!error.response) {
      return Promise.reject(new Error('Erro de rede: Não foi possível conectar ao servidor'))
    }

    const { status, data } = error.response
    
    switch (status) {
      case 404:
        return Promise.reject(new Error(data.detail || 'Recurso não encontrado'))
      case 500:
        return Promise.reject(new Error('Erro interno do servidor'))
      default:
        return Promise.reject(new Error(data.detail || 'Erro desconhecido'))
    }
  }
)

export default api
