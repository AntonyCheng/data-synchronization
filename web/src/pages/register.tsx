import { LockOutlined, SafetyOutlined, UserOutlined } from '@ant-design/icons';
import { LoginForm, ProFormText } from '@ant-design/pro-components';
import { history } from '@umijs/max';
import { Button, ConfigProvider, message, Modal } from 'antd';
import enUS from 'antd/locale/en_US';
import zhCN from 'antd/locale/zh_CN';
import { useCallback, useEffect, useState } from 'react';
import type { RegisterParams, VerifyCodeResult } from '@/api/types';
import { getCodeImg, register } from '@/api/login';
import { useLoading } from '@/hooks/useLoading';
import { useAppStore } from '@/stores/appStore';
import { appEnv } from '@/utils/env';

const registerText = {
  zh_CN: {
    brandTitle: '一站式实时计算平台',
    brandDesc:
      '统一管理数据源、同步任务、实时链路与运行质量，快速搭建可靠的数据同步工作台。',
    highlights: ['数据源统一管理', '任务全生命周期', '多表整库同步', '运行质量可视'],
    formSubTitle: '创建新的业务工作台账号',
    username: '账号',
    password: '密码',
    confirmPassword: '确认密码',
    code: '验证码',
    submit: '注册',
    submitting: '注册中...',
    login: '使用已有账号登录',
    tip: '注册后将返回登录页继续认证',
    successTitle: '系统提示',
    success: (username?: string) => `恭喜你，账号 ${username} 注册成功！`,
    fail: '注册失败',
    usernameRequired: '请输入账号',
    usernameLength: '账号长度必须介于 2 和 20 之间',
    passwordRequired: '请输入密码',
    passwordLength: '密码长度必须介于 5 和 20 之间',
    passwordPattern: '不能包含非法字符：< > " \' \\ |',
    confirmRequired: '请确认密码',
    confirmMismatch: '两次输入的密码不一致',
    codeRequired: '请输入验证码'
  },
  en_US: {
    brandTitle: 'One-stop Real-time Computing Platform',
    brandDesc:
      'Manage data sources, synchronization tasks, real-time pipelines and runtime quality from one workspace.',
    highlights: ['Unified sources', 'Task lifecycle', 'Multi-table sync', 'Visible runtime quality'],
    formSubTitle: 'Create a new workspace account',
    username: 'Username',
    password: 'Password',
    confirmPassword: 'Confirm password',
    code: 'Captcha',
    submit: 'Register',
    submitting: 'Registering...',
    login: 'Use existing account',
    tip: 'After registration, return to sign in and continue authentication',
    successTitle: 'System Message',
    success: (username?: string) => `Account ${username} registered successfully.`,
    fail: 'Registration failed',
    usernameRequired: 'Please enter username',
    usernameLength: 'Username must be between 2 and 20 characters',
    passwordRequired: 'Please enter password',
    passwordLength: 'Password must be between 5 and 20 characters',
    passwordPattern: 'Invalid characters are not allowed: < > " \' \\ |',
    confirmRequired: 'Please confirm password',
    confirmMismatch: 'The two passwords do not match',
    codeRequired: 'Please enter captcha'
  }
};

export default function Register() {
  const appLocale = useAppStore(state => state.appLocale);
  const [captcha, setCaptcha] = useState<VerifyCodeResult>({ captchaEnabled: true });
  const { loading, withLoading } = useLoading();
  const text = registerText[appLocale];

  const loadCaptcha = useCallback(async () => {
    const res = await getCodeImg();
    setCaptcha({
      ...res.data,
      captchaEnabled: res.data.captchaEnabled === undefined ? true : res.data.captchaEnabled
    });
  }, []);

  useEffect(() => {
    loadCaptcha().catch(() => setCaptcha({ captchaEnabled: false }));
  }, [loadCaptcha]);

  useEffect(() => {
    document.title = appEnv.title;
  }, []);

  const submitRegister = async (values: RegisterParams) => {
    await withLoading(async () => {
      try {
        await register({
          ...values,
          uuid: captcha.uuid,
          userType: 'sys_user'
        });
        await Modal.success({
          title: text.successTitle,
          content: text.success(values.username)
        });
        history.push('/login');
      } catch (error) {
        if (captcha.captchaEnabled) {
          loadCaptcha();
        }
        message.error((error as Error).message || text.fail);
      }
    });
  };

  return (
    <ConfigProvider locale={appLocale === 'zh_CN' ? zhCN : enUS}>
      <div className="register-page">
        <section className="register-brand-react">
          <span className="register-brand-pill">实时计算工作台</span>
          <h1>{text.brandTitle}</h1>
          <p>{text.brandDesc}</p>
          <div className="register-highlights">
            {text.highlights.map(item => (
              <span key={item}>{item}</span>
            ))}
          </div>
        </section>

        <section className="register-panel">
          <LoginForm<RegisterParams>
            title={appEnv.logoTitle}
            subTitle={text.formSubTitle}
            submitter={{
              searchConfig: { submitText: loading ? text.submitting : text.submit },
              submitButtonProps: { loading, block: true, size: 'large' }
            }}
            onFinish={async values => {
              await submitRegister(values);
              return true;
            }}
          >
            <ProFormText
              name="username"
              fieldProps={{ size: 'large', prefix: <UserOutlined /> }}
              placeholder={text.username}
              rules={[
                { required: true, message: text.usernameRequired },
                { min: 2, max: 20, message: text.usernameLength }
              ]}
            />
            <ProFormText.Password
              name="password"
              fieldProps={{ size: 'large', prefix: <LockOutlined /> }}
              placeholder={text.password}
              rules={[
                { required: true, message: text.passwordRequired },
                { min: 5, max: 20, message: text.passwordLength },
                { pattern: /^[^<>"'|\\]+$/, message: text.passwordPattern }
              ]}
            />
            <ProFormText.Password
              name="confirmPassword"
              fieldProps={{ size: 'large', prefix: <LockOutlined /> }}
              placeholder={text.confirmPassword}
              dependencies={['password']}
              rules={[
                { required: true, message: text.confirmRequired },
                ({ getFieldValue }) => ({
                  validator(_, value) {
                    if (!value || getFieldValue('password') === value) return Promise.resolve();
                    return Promise.reject(new Error(text.confirmMismatch));
                  }
                })
              ]}
            />
            {captcha.captchaEnabled && (
              <div className="captcha-row">
                <ProFormText
                  name="code"
                  fieldProps={{ size: 'large', prefix: <SafetyOutlined /> }}
                  placeholder={text.code}
                  rules={[{ required: true, message: text.codeRequired }]}
                />
                <button type="button" className="captcha-button" onClick={loadCaptcha}>
                  <img className="captcha-image" src={`data:image/gif;base64,${captcha.img}`} alt="captcha" />
                </button>
              </div>
            )}
            <div className="register-actions">
              <span>{text.tip}</span>
              <Button type="link" onClick={() => history.push('/login')}>
                {text.login}
              </Button>
            </div>
          </LoginForm>
        </section>
      </div>
    </ConfigProvider>
  );
}
