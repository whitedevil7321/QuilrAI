# Rate_Limiting/Rate_limiter.py

import asyncio
import math
import sqlite3
import threading
import time


class RateLimiter:
    def __init__(
        self,
        db_path="gateway.db",
        limit=50_000,
        window_seconds=60
    ):
        self.limit = limit
        self.window = window_seconds
        self.lock = threading.Lock()

        self.db = sqlite3.connect(
            db_path,
            check_same_thread=False,
            timeout=30
        )

        self.db.execute("PRAGMA journal_mode=WAL")

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS usage (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant  TEXT    NOT NULL,
                time    REAL    NOT NULL,
                tokens  INTEGER NOT NULL
            )
        """)

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_tenant_time
            ON usage (tenant, time)
        """)

        self.db.commit()

    def _validate_tokens(self, tokens, allow_zero=False):
        if isinstance(tokens, bool) or not isinstance(tokens, int):
            raise TypeError("tokens must be an integer")

        minimum = 0 if allow_zero else 1

        if tokens < minimum:
            raise ValueError(f"tokens must be at least {minimum}")

    def _try_reserve(self, tenant, tokens):
        """Return (allowed, row_id, retry_after_seconds)."""

        self._validate_tokens(tokens)

        if not tenant:
            raise ValueError("tenant cannot be empty")

        now = time.time()
        window_start = now - self.window

        with self.lock:

            # 1. Remove expired usage for this tenant.
            self.db.execute(
                """
                DELETE FROM usage
                WHERE tenant = ? AND time <= ?
                """,
                (tenant, window_start)
            )

            # 2. Calculate usage within the current window.
            used = self.db.execute(
                """
                SELECT COALESCE(SUM(tokens), 0)
                FROM usage
                WHERE tenant = ?
                """,
                (tenant,)
            ).fetchone()[0]

            # 3. Allow the request if it fits within the limit.
            if used + tokens <= self.limit:
                cursor = self.db.execute(
                    """
                    INSERT INTO usage (tenant, time, tokens)
                    VALUES (?, ?, ?)
                    """,
                    (tenant, now, tokens)
                )

                self.db.commit()

                return True, cursor.lastrowid, 0

            # 4. Otherwise, calculate when enough usage expires.
            self.db.commit()

            retry_after = self._seconds_until_free(
                tenant,
                used + tokens - self.limit,
                now
            )

            return False, None, retry_after

    def _seconds_until_free(self, tenant, tokens_needed, now):
        """Find when enough old tokens will expire."""

        freed = 0

        rows = self.db.execute(
            """
            SELECT time, tokens
            FROM usage
            WHERE tenant = ?
            ORDER BY time
            """,
            (tenant,)
        ).fetchall()

        for row_time, row_tokens in rows:
            freed += row_tokens

            if freed >= tokens_needed:
                return max(
                    1,
                    math.ceil(row_time + self.window - now)
                )

        return self.window

    def _set_tokens(self, row_id, tokens):
        """Replace estimated tokens with actual token usage."""

        self._validate_tokens(tokens, allow_zero=True)

        with self.lock:
            self.db.execute(
                """
                UPDATE usage
                SET tokens = ?
                WHERE id = ?
                """,
                (tokens, row_id)
            )

            self.db.commit()

    def _remove(self, row_id):
        """Remove a reservation when a request fails."""

        with self.lock:
            self.db.execute(
                "DELETE FROM usage WHERE id = ?",
                (row_id,)
            )

            self.db.commit()

    def _used(self, tenant):
        """Return current token usage for a tenant."""

        with self.lock:
            window_start = time.time() - self.window

            return self.db.execute(
                """
                SELECT COALESCE(SUM(tokens), 0)
                FROM usage
                WHERE tenant = ? AND time > ?
                """,
                (tenant, window_start)
            ).fetchone()[0]

    def _evict_all(self):
        """Delete expired usage for all tenants."""

        with self.lock:
            self.db.execute(
                "DELETE FROM usage WHERE time <= ?",
                (time.time() - self.window,)
            )

            self.db.commit()

    # --------------------------------------------------
    # Async methods
    # --------------------------------------------------

    async def try_reserve(self, tenant, tokens):
        return await asyncio.to_thread(
            self._try_reserve,
            tenant,
            tokens
        )

    async def set_tokens(self, row_id, tokens):
        await asyncio.to_thread(
            self._set_tokens,
            row_id,
            tokens
        )

    async def remove(self, row_id):
        await asyncio.to_thread(
            self._remove,
            row_id
        )

    async def used(self, tenant):
        return await asyncio.to_thread(
            self._used,
            tenant
        )

    async def evict_all(self):
        await asyncio.to_thread(
            self._evict_all
        )

    def close(self):
        with self.lock:
            self.db.close()