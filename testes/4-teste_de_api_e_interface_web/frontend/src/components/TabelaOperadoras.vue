<template>
  <div class="table-container">
    <table>
      <thead>
        <tr>
          <th>Registro ANS</th>
          <th>CNPJ</th>
          <th>Razão Social</th>
          <th>Modalidade</th>
          <th>UF</th>
          <th>Status</th>
          <th>Ações</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="operadora in operadoras" :key="operadora.reg_ans">
          <td>{{ operadora.reg_ans }}</td>
          <td>{{ formatarCnpj(operadora.cnpj) }}</td>
          <td>{{ operadora.razao_social }}</td>
          <td>{{ operadora.modalidade || 'N/A' }}</td>
          <td>{{ operadora.uf || 'N/A' }}</td>
          <td>
            <span :class="badgeClass(operadora.status)">
              {{ operadora.status }}
            </span>
          </td>
          <td>
            <router-link
              :to="`/operadora/${operadora.cnpj}`"
              class="link"
            >
              Ver detalhes
            </router-link>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
export default {
  name: 'TabelaOperadoras',
  props: {
    operadoras: {
      type: Array,
      required: true
    }
  },
  setup() {
    const formatarCnpj = (cnpj) => {
      if (!cnpj) return ''
      return cnpj.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/, '$1.$2.$3/$4-$5')
    }

    const badgeClass = (status) => {
      const classes = {
        'ATIVA': 'badge badge-success',
        'CANCELADA': 'badge badge-danger',
        'SUSPENSA': 'badge badge-warning'
      }
      return classes[status] || 'badge'
    }

    return {
      formatarCnpj,
      badgeClass
    }
  }
}
</script>
