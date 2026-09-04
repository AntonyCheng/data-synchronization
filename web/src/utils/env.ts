const env = import.meta.env;

export const appEnv = {
  title: env.VITE_APP_TITLE || '一站式实时计算平台',
  logoTitle: env.VITE_APP_LOGO_TITLE || '一站式实时计算平台',
  baseApi: env.VITE_APP_BASE_API || '',
  contextPath: env.VITE_APP_CONTEXT_PATH || '/',
  clientId: env.VITE_APP_CLIENT_ID || '',
  encryptEnabled: env.VITE_APP_ENCRYPT === 'true',
  rsaPublicKey: env.VITE_APP_RSA_PUBLIC_KEY || '',
  rsaPrivateKey: env.VITE_APP_RSA_PRIVATE_KEY || '',
  messageEnabled: env.VITE_APP_MESSAGE_ENABLED !== 'false',
  messageTransport: (env.VITE_APP_MESSAGE_TRANSPORT || 'sse').toLowerCase(),
  messagePath: env.VITE_APP_MESSAGE_PATH || '/resource/message',
  monitorAdmin: env.VITE_APP_MONITOR_ADMIN || '',
  snailJobAdmin: env.VITE_APP_SNAILJOB_ADMIN || ''
};
