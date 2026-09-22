-- Table-level reinitialize for task groups: POST /sync/group/{groupId}/item/{itemId}/reinitialize
-- discards one isolated table's engine job and savepoint and rebuilds it from a fresh snapshot,
-- leaving the other tables untouched. Button permission under the task-group menu; granted to
-- the same role the other task-group buttons use. Safe to run repeatedly against ry-vue.
insert ignore into sys_menu values
(1761400000000002065, '多表重新初始化表项', 1761400000000002050, 15, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:reinitialize', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '丢弃单个表项的作业与 savepoint 并重新全量同步');
insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id = 1761400000000002065;
