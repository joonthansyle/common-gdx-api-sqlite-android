/**<p>*********************************************************************************************************************
 * <h1>BuildSqlSelect</h1>
 * @since 20230327
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20230327  Original Author       evanwht1@gmail.com
 *           modified              @author...
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 * E501, E503, E504
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.android.builder;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.sql.DatabaseCursor;
import com.badlogic.gdx.sql.SQLiteGdxException;
import com.badlogic.gdx.sql.SqliteDataTypes;
import com.badlogic.gdx.sql.builder.Column;
import com.badlogic.gdx.sql.builder.ResultMapper;
import com.badlogic.gdx.sql.builder.SqlBuilderSelect;
import com.badlogic.gdx.sqlite.android.AndroidCursor;

import java.sql.Connection;
import java.util.Map;

public class BuildSqlSelect extends SqlBuilderSelect {
    private static final String TAG = BuildSqlSelect.class.getCanonicalName();
    private SQLiteDatabase db;
    private String androidSql;

    /* ERRORS */
    private final String E501 = "Unknown Sqlite DataType, use SqliteDataTypes";
    private final String E503 = "Operating System not Supported";
    private final String E504 = "Database is not an instance of SQLiteDatabase";
    public BuildSqlSelect(ResultMapper resultMapper) {
        super(resultMapper);
    }

    @Override
    protected Object preparedStatementAndroid(Object androidDatabase) throws SQLiteGdxException {
        if(!(androidDatabase instanceof SQLiteDatabase)) throw new SQLiteGdxException(E504);
        db  = (SQLiteDatabase) androidDatabase;
        androidSql = createStatement();
        SQLiteStatement aStatement = db.compileStatement(androidSql);
        int index = 1;
        for (Map.Entry<Column, Object> p : clauses.entrySet()) {
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

                return aStatement;
            }
        }

        return null;
    }

    /* Connection not Supported in Android */
    @Override
    protected Object preparedStatementWin(Connection connection) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }

    /* Connection not Supported in Android */
    @Override
    public DatabaseCursor getCursor(Connection connection) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }

    /* Connection not Supported in Android */
    @Override
    public DatabaseCursor getCursor(DatabaseCursor cursor, Connection connection) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }

    @Override
    public DatabaseCursor getCursor(Object androidDatabase) throws SQLiteGdxException {
        AndroidCursor aCursor = new AndroidCursor();
        preparedStatementAndroid(androidDatabase);
        Cursor tmp = db.rawQuery(androidSql, null);
        aCursor.setNativeCursor(tmp);
        return aCursor;
    }


    @Override
    public DatabaseCursor getCursor(DatabaseCursor cursor, Object androidDatabase) throws SQLiteGdxException {
        AndroidCursor aCursor = (AndroidCursor) cursor;
        preparedStatementAndroid(androidDatabase);
        Cursor tmp = db.rawQuery(androidSql, null);
        aCursor.setNativeCursor(tmp);
        return aCursor;
    }
}
