<template>
  <div style="max-width: 800px; margin: 0 auto;">
    <Bar :data="chartData" :options="chartOptions" />
  </div>
</template>

<script>
import { computed } from 'vue'
import { Bar } from 'vue-chartjs'
import {
  Chart as ChartJS,
  Title,
  Tooltip,
  Legend,
  BarElement,
  CategoryScale,
  LinearScale
} from 'chart.js'

ChartJS.register(Title, Tooltip, Legend, BarElement, CategoryScale, LinearScale)

export default {
  name: 'GraficoDespesasUf',
  components: {
    Bar
  },
  props: {
    dados: {
      type: Array,
      required: true
    }
  },
  setup(props) {
    const chartData = computed(() => {
      const labels = props.dados.map(d => d.uf)
      const values = props.dados.map(d => d.total_despesas)

      return {
        labels,
        datasets: [
          {
            label: 'Total de Despesas (R$)',
            data: values,
            backgroundColor: 'rgba(59, 130, 246, 0.6)',
            borderColor: 'rgba(59, 130, 246, 1)',
            borderWidth: 1
          }
        ]
      }
    })

    const chartOptions = {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: {
          display: true,
          position: 'top'
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              let label = context.dataset.label || ''
              if (label) {
                label += ': '
              }
              label += new Intl.NumberFormat('pt-BR', {
                style: 'currency',
                currency: 'BRL'
              }).format(context.parsed.y)
              return label
            }
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              return new Intl.NumberFormat('pt-BR', {
                style: 'currency',
                currency: 'BRL',
                notation: 'compact'
              }).format(value)
            }
          }
        }
      }
    }

    return {
      chartData,
      chartOptions
    }
  }
}
</script>
