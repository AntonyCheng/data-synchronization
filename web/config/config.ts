import { defineConfig } from '@umijs/max';
import { createUmiAppConfig } from '../vite.config';

const appConfig = createUmiAppConfig();

export default defineConfig({
  title: appConfig.title,
  antd: {},
  access: {},
  model: {},
  initialState: {},
  vite: appConfig.vite,
  // 关闭 MFSU：使用 Vite 自带的 esbuild 依赖预构建即可。
  // MFSU 的 eager worker 冷启动要 6~22s，且在非 ASCII 家目录下会触发
  // "The request url \"C:/\" is outside of Vite serving allow list" 崩溃。
  mfsu: false,
  npmClient: 'pnpm',
  hash: true,
  esbuildMinifyIIFE: true,
  history: {
    type: 'browser'
  },
  base: appConfig.base,
  publicPath: appConfig.publicPath,
  routes: [
    { path: '/login', component: './login', layout: false },
    { path: '/register', component: './register', layout: false },
    { path: '/social-callback', component: './socialCallback', layout: false },
    { path: '/401', component: './error/401', layout: false },
    { path: '/404', component: './error/404', layout: false },
    { path: '/redirect/*', component: './redirect', layout: false },
    {
      path: '/',
      component: '../layouts/BasicLayout',
      routes: [
        { path: '/', redirect: '/index' },
        { path: '/index', component: './index' },
        { path: '/user/profile', component: './system/user/profile' },
        { path: '*', component: './dynamicPage' }
      ]
    }
  ],
  proxy: appConfig.proxy
});
