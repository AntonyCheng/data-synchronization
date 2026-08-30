-- Recovery boundary and reinitialization permission migration.
-- Safe to run repeatedly against the platform metadata database.

insert ignore into sys_menu values
(1761400000000002036, '任务重新初始化', 1761400000000002002, 13, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:reinitialize', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');

insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, 1761400000000002036
where exists (select 1 from sys_role where role_id = 1761300000000000003);
