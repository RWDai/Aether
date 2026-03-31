<template>
  <svg
    xmlns="http://www.w3.org/2000/svg"
    :viewBox="viewBox"
    fill="currentColor"
  >
    <path
      :d="aetherFullPath"
      fill-rule="evenodd"
      stroke="none"
    />
  </svg>
</template>

<script setup lang="ts">
import { AETHER_FULL_PATH, AETHER_SVG_VIEWBOX } from '@/constants/logoPaths'

const aetherFullPath = AETHER_FULL_PATH
const viewBox = AETHER_SVG_VIEWBOX
</script>
