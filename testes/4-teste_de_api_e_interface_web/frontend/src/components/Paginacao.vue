<template>
  <div class="pagination">
    <button
      @click="$emit('mudar-pagina', paginaAtual - 1)"
      :disabled="paginaAtual === 1"
    >
      Anterior
    </button>
    
    <span>
      Página {{ paginaAtual }} de {{ totalPaginas }} ({{ total }} registros)
    </span>
    
    <button
      @click="$emit('mudar-pagina', paginaAtual + 1)"
      :disabled="paginaAtual >= totalPaginas"
    >
      Próxima
    </button>
  </div>
</template>

<script>
import { computed } from 'vue'

export default {
  name: 'Paginacao',
  props: {
    paginaAtual: {
      type: Number,
      required: true
    },
    total: {
      type: Number,
      required: true
    },
    limite: {
      type: Number,
      required: true
    }
  },
  emits: ['mudar-pagina'],
  setup(props) {
    const totalPaginas = computed(() => {
      return Math.ceil(props.total / props.limite)
    })

    return {
      totalPaginas
    }
  }
}
</script>
