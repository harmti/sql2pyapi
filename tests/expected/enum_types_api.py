from enum import Enum
from typing import List, Optional, AsyncIterable

from asyncpg import Connection as AsyncConnection


class StatusType(Enum):
    PENDING = 'pending'
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    DELETED = 'deleted'


class UserRole(Enum):
    ADMIN = 'admin'
    MODERATOR = 'moderator'
    USER = 'user'
    GUEST = 'guest'


@dataclass
class GetUsersByStatusResult:
    user_id: int
    username: str
    status: StatusType
    role: UserRole


async def get_default_status(conn: AsyncConnection) -> StatusType:
    """Call PostgreSQL function get_default_status()."""
    sql = """SELECT get_default_status();"""
    result = await conn.fetchval(sql)
    return StatusType(result)


async def is_active_role(conn: AsyncConnection, role: UserRole) -> bool:
    """Call PostgreSQL function is_active_role()."""
    sql = """SELECT is_active_role($1);"""
    result = await conn.fetchval(sql, role.value)
    return result


async def get_users_by_status(conn: AsyncConnection, status: StatusType) -> List[GetUsersByStatusResult]:
    """Call PostgreSQL function get_users_by_status()."""
    sql = """SELECT * FROM get_users_by_status($1);"""
    rows = await conn.fetch(sql, status.value)
    return [
        GetUsersByStatusResult(
            user_id=row["user_id"],
            username=row["username"],
            status=StatusType(row["status"]),
            role=UserRole(row["role"])
        )
        for row in rows
    ]
