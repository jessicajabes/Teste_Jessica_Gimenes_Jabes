import api from './api'

export const operadorasService = {
  async listar(page = 1, limit = 10, busca = '') {
    const params = { page, limit }
    if (busca) params.q = busca
    
    const response = await api.get('/api/operadoras', { params })
    return response.data
  },

  async obterPorCnpj(cnpj) {
    const response = await api.get(`/api/operadoras/${cnpj}`)
    return response.data
  },

  async obterDespesas(cnpj) {
    const response = await api.get(`/api/operadoras/${cnpj}/despesas`)
    return response.data
  },

  async obterEstatisticas() {
    const response = await api.get('/api/estatisticas')
    return response.data
  }
}
