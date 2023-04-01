/**<p>*********************************************************************************************************************
 * <h1>AndroidDatabaseManager</h1>
 * @since 20150926
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20150926  Initial version       @author M Rafay Aleem
 * 20230323  "String sql" might be subject for SQL Injection, if this will be the case it has to be deprecated.
 *           This needs to be modified and use other alternatives, such as Prepared Statement, etc.
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 *
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.android;

import android.content.Context;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.backends.android.AndroidApplication;
import com.badlogic.gdx.sql.Database;
import com.badlogic.gdx.sql.DatabaseCursor;
import com.badlogic.gdx.sql.DatabaseManager;
import com.badlogic.gdx.sql.SQLiteGdxException;
import com.badlogic.gdx.sql.builder.*;

import java.util.OptionalInt;
import java.util.OptionalLong;

public class AndroidDatabaseManager implements DatabaseManager {

	private Context context;

	private class AndroidDatabase implements Database {

		private SQLiteDatabaseHelper helper;
		private SQLiteDatabase database;
		private Context context;

		private final String dbName;
		private final int dbVersion;
		private final String dbOnCreateQuery;
		private final String dbOnUpgradeQuery;

		private AndroidDatabase (Context context, String dbName, int dbVersion, String dbOnCreateQuery, String dbOnUpgradeQuery) {
			this.context = context;
			this.dbName = dbName;
			this.dbVersion = dbVersion;
			this.dbOnCreateQuery = dbOnCreateQuery;
			this.dbOnUpgradeQuery = dbOnUpgradeQuery;
		}

		@Override
		public void setupDatabase () {
			helper = new SQLiteDatabaseHelper(this.context, dbName, null, dbVersion, dbOnCreateQuery, dbOnUpgradeQuery);
		}

		@Override
		public void openOrCreateDatabase () throws SQLiteGdxException {
			try {
				database = helper.getWritableDatabase();
			} catch (SQLiteException e) {
				throw new SQLiteGdxException(e);
			}
		}

		@Override
		public void closeDatabase () throws SQLiteGdxException {
			try {
				helper.close();
			} catch (SQLiteException e) {
				throw new SQLiteGdxException(e);
			}
		}

		@Override
        @Deprecated
		public void execSQL (String sql) throws SQLiteGdxException {
			try {
				database.execSQL(sql);
			} catch (SQLException e) {
				throw new SQLiteGdxException(e);
			}
		}

		@Override
        @Deprecated
		public DatabaseCursor rawQuery(String sql) throws SQLiteGdxException {
			AndroidCursor aCursor = new AndroidCursor();
			try {
				Cursor tmp = database.rawQuery(sql, null);
				aCursor.setNativeCursor(tmp);
				return aCursor;
			} catch (SQLiteException e) {
				throw new SQLiteGdxException(e);
			}
		}

		@Override
		public DatabaseCursor rawQuery (DatabaseCursor cursor, String sql) throws SQLiteGdxException {
			AndroidCursor aCursor = (AndroidCursor)cursor;
			try {
				Cursor tmp = database.rawQuery(sql, null);
				aCursor.setNativeCursor(tmp);
				return aCursor;
			} catch (SQLiteException e) {
				throw new SQLiteGdxException(e);
			}
		}

        @Override
        public DatabaseCursor getCursor(SqlBuilderSelect builder) throws SQLiteGdxException {
            return builder.getCursor(database);
        }

        @Override
        public DatabaseCursor getCursor(DatabaseCursor cursor, SqlBuilderSelect builder) throws SQLiteGdxException {
            return builder.getCursor(cursor, database);
        }

        @Override
        public OptionalLong insert(SqlBuilderInsert builder) throws SQLiteGdxException {
            return builder.insert(database);
        }

        @Override
        public OptionalInt delete(SqlBuilderDelete builder) throws SQLiteGdxException, java.sql.SQLException {
            return builder.delete(database);
        }

        @Override
        public OptionalInt update(SqlBuilderUpdate builder) throws SQLiteGdxException, java.sql.SQLException {
            return builder.update(database);
        }
    }

	public AndroidDatabaseManager () {
		AndroidApplication app = (AndroidApplication)Gdx.app;
		context = app.getApplicationContext();
	}

	@Override
	public Database getNewDatabase (String databaseName, int databaseVersion, String databaseCreateQuery, String dbOnUpgradeQuery) {
		return new AndroidDatabase(this.context, databaseName, databaseVersion, databaseCreateQuery, dbOnUpgradeQuery);
	}

}
