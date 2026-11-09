-- 生产环境加固：安装与每次升级时由 migrate.sh 在结构迁移之后执行，可重复执行。
-- 只处理初始化数据（ry_vue.sql）原样留下的演示内容，管理员改过的一律不动。

-- 1. 演示账号 test、test1：出厂口令 666666，test 的角色还带全部数据同步权限（ry_sync.sql）。
--    只删除仍与初始化数据一致的账号：用户 ID、用户名、出厂口令三者都对上。
DELETE ur FROM sys_user_role ur
  JOIN sys_user u ON u.user_id = ur.user_id
 WHERE u.user_id IN (1761100000000000003, 1761100000000000004)
   AND u.user_name IN ('test', 'test1')
   AND u.password = '$2a$10$b8yUzN0C71sbz.PhNOCgJe.Tu1yWC3RNrTyjSQ8p1W0.aaUXUJ.Ne';
DELETE FROM sys_user
 WHERE user_id IN (1761100000000000003, 1761100000000000004)
   AND user_name IN ('test', 'test1')
   AND password = '$2a$10$b8yUzN0C71sbz.PhNOCgJe.Tu1yWC3RNrTyjSQ8p1W0.aaUXUJ.Ne';

-- 2. admin 的资料仍是上游作者的昵称、邮箱和手机号时，换成通用值。
UPDATE sys_user SET nick_name = '管理员', email = '', phone_number = ''
 WHERE user_id = 1761100000000000001 AND user_name = 'admin' AND nick_name = '疯狂的狮子Li';
