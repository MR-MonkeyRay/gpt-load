import { QueryClient } from '@tanstack/vue-query'

// 后台运行中的快照需要按固定节奏回看：全部后台工作结束立即停表，不做长期轮询。
// 刷新策略只在这里定义，页面不覆盖。
const runningPollIntervalMs = 2_000

// 一个快照只要报告还有后台工作在跑就继续回看：running 是当前回看的运行，
// runningModels 是同一凭据上其他模型的运行——只看前者会让别处的进度停住。
export const runningPolling = {
  refetchInterval: (query: {
    state: { data?: { running: boolean; runningModels?: string[] } }
  }): number | false =>
    query.state.data?.running || query.state.data?.runningModels?.length
      ? runningPollIntervalMs
      : false,
}

export function createModernQueryClient() {
  return new QueryClient({
    defaultOptions: {
      // 使用查询库默认的 visibilitychange 监听与页面挂载刷新，不监听窗口 focus。
      // staleTime 只决定事件触发时是否更新缓存，不会产生定时请求。
      queries: {
        retry: false,
        staleTime: 0,
        refetchInterval: false,
        refetchIntervalInBackground: false,
        refetchOnWindowFocus: true,
        refetchOnReconnect: false,
        refetchOnMount: true,
      },
      mutations: { retry: false },
    },
  })
}
