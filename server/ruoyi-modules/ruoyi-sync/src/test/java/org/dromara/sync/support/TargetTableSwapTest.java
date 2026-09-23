package org.dromara.sync.support;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The FULL/OVERWRITE cutover renames tables in a customer's target database, so the exact
 * statements matter more than anything else in this module: a wrong order loses the live table,
 * and a split MySQL rename exposes a window where the target does not exist.
 */
@Tag("dev")
class TargetTableSwapTest {

    @Test
    void postgresReplacesTheLiveTableThroughABackupAndDropsItAfterwards() {
        assertEquals(List.of(
            "drop table if exists \"public\".\"orders__bak\"",
            "alter table \"public\".\"orders\" rename to \"orders__bak\"",
            "alter table \"public\".\"orders__stage\" rename to \"orders\"",
            "drop table if exists \"public\".\"orders__bak\""
        ), TargetTableSwap.postgresPlan("public", "orders", "orders__stage", "orders__bak", true));
    }

    @Test
    void postgresSkipsTheBackupRenameWhenTheTargetDoesNotExistYet() {
        assertEquals(List.of(
            "drop table if exists \"public\".\"orders__bak\"",
            "alter table \"public\".\"orders__stage\" rename to \"orders\"",
            "drop table if exists \"public\".\"orders__bak\""
        ), TargetTableSwap.postgresPlan("public", "orders", "orders__stage", "orders__bak", false));
    }

    @Test
    void postgresRenamesToABareName() {
        // "ALTER TABLE x RENAME TO schema.y" is a syntax error in PostgreSQL: the new name must
        // not be qualified. Only the table being renamed carries the schema.
        List<String> plan = TargetTableSwap.postgresPlan("analytics", "orders", "orders__stage", "orders__bak", true);
        assertTrue(plan.get(1).endsWith("rename to \"orders__bak\""), plan.get(1));
        assertFalse(plan.get(1).contains("to \"analytics\""), plan.get(1));
        assertTrue(plan.get(2).startsWith("alter table \"analytics\".\"orders__stage\""), plan.get(2));
    }

    @Test
    void mysqlPutsBothRenamesInOneAtomicStatement() {
        List<String> plan = TargetTableSwap.mysqlPlan("sink_db", "orders", "orders__stage", "orders__bak", true);

        assertEquals(List.of(
            "DROP TABLE IF EXISTS `sink_db`.`orders__bak`",
            "RENAME TABLE `sink_db`.`orders` TO `sink_db`.`orders__bak`,"
                + " `sink_db`.`orders__stage` TO `sink_db`.`orders`",
            "DROP TABLE IF EXISTS `sink_db`.`orders__bak`"
        ), plan);
        // MySQL DDL is not transactional; splitting these would leave the target missing in between.
        assertEquals(1, plan.stream().filter(sql -> sql.startsWith("RENAME TABLE")).count());
    }

    @Test
    void mysqlSkipsTheBackupEntirelyWhenTheTargetDoesNotExistYet() {
        assertEquals(List.of(
            "DROP TABLE IF EXISTS `sink_db`.`orders__bak`",
            "RENAME TABLE `sink_db`.`orders__stage` TO `sink_db`.`orders`"
        ), TargetTableSwap.mysqlPlan("sink_db", "orders", "orders__stage", "orders__bak", false));
    }

    @Test
    void aLeftoverBackupIsAlwaysClearedBeforeAnyRename() {
        // A backup from an earlier failed swap would make the rename fail, stranding the task.
        for (boolean targetExists : new boolean[]{true, false}) {
            List<String> pg = TargetTableSwap.postgresPlan("public", "t", "t__stage", "t__bak", targetExists);
            assertTrue(pg.get(0).startsWith("drop table if exists"), pg.toString());
            List<String> mysql = TargetTableSwap.mysqlPlan("db", "t", "t__stage", "t__bak", targetExists);
            assertTrue(mysql.get(0).startsWith("DROP TABLE IF EXISTS"), mysql.toString());
        }
    }

    @Test
    void identifiersAreEscapedSoATableNameCannotBreakOutOfItsQuotes() {
        // Target names reach here from user input; a stray quote must be doubled, not terminate
        // the identifier.
        List<String> pg = TargetTableSwap.postgresPlan("we\"ird", "ta\"ble", "st\"age", "ba\"k", true);
        assertTrue(pg.get(1).contains("\"we\"\"ird\".\"ta\"\"ble\""), pg.get(1));
        assertTrue(pg.get(1).endsWith("rename to \"ba\"\"k\""), pg.get(1));

        List<String> mysql = TargetTableSwap.mysqlPlan("d`b", "ta`ble", "st`age", "ba`k", true);
        assertTrue(mysql.get(1).contains("`d``b`.`ta``ble`"), mysql.get(1));
        assertTrue(mysql.get(1).contains("`d``b`.`st``age`"), mysql.get(1));
    }
}
