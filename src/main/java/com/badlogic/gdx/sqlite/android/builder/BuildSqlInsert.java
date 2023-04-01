/**<p>*********************************************************************************************************************
 * <h1>BuildSqlInsert</h1>
 * @since 20230328
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20230328  original author       evanwht1@gmail.com
 * 20230329  TODO: verify the use of transaction
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 * E501, E502, E503, E504
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.android.builder;

import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.sql.SQLiteGdxException;
import com.badlogic.gdx.sql.SqliteDataTypes;
import com.badlogic.gdx.sql.builder.Column;
import com.badlogic.gdx.sql.builder.SqlBuilderInsert;

import java.sql.Connection;
import java.util.Map;
import java.util.OptionalLong;

public class BuildSqlInsert extends SqlBuilderInsert {
    private static final String TAG = BuildSqlInsert.class.getCanonicalName();
    public static final String NAME = TAG;

    private SQLiteDatabase db;
    private String androidSql;

    private final String E501 = "Unknown Sqlite DataType, use SqliteDataTypes";
    private final String E502 = "Table is undefined";
    private final String E503 = "Operating System not Supported";
    private final String E504 = "Database is not an instance of SQLiteDatabase";

    /** Runs on Windows */
    @Override
    public OptionalLong insert(Connection connection) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }

    @Override
    public OptionalLong insert(Object androidDatabase) throws SQLiteGdxException {
        if (table == null) throw new SQLiteGdxException(E502);
        if(!(androidDatabase instanceof SQLiteDatabase)) throw new SQLiteGdxException(E504);
        db  = (SQLiteDatabase) androidDatabase;
        androidSql = createStatement();
        db.beginTransaction();
        int index = 1;
        SQLiteStatement aStatement = db.compileStatement(androidSql);
        for (Map.Entry<Column, Object> p : values.entrySet()) {
            if (p!= null) {
                aStatement.clearBindings();
                /* Based on SqliteDataTypes */
                switch (p.getKey().getType()) {
                    case SqliteDataTypes.BLOB:
                        aStatement.bindBlob(index++, (byte[]) p.getValue());
                        break;
                    case SqliteDataTypes.DOUBLE:
                        aStatement.bindDouble(index++, Double.parseDouble(String.valueOf(p.getValue())));
                        break;
                    case SqliteDataTypes.LONG:
                        aStatement.bindLong(index++,Long.parseLong(String.valueOf(p.getValue())));
                        break;
                    case SqliteDataTypes.STRING:
                        aStatement.bindString(index++, String.valueOf(p.getValue()));
                        break;
                    case SqliteDataTypes.NULL:
                        aStatement.bindNull(index++);
                        break;
                    default:
                        Gdx.app.error(TAG, E501);
                        break;
                }
            }
        }
        OptionalLong optionalLong = OptionalLong.of(aStatement.executeInsert());
        db.setTransactionSuccessful();
        db.endTransaction();
        return optionalLong;
    }
}
