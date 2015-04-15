DatabaseErrors = []


def register(exception_class):
    DatabaseErrors.append(exception_class)


class BadMigration(Exception):
    """
    The migration file could not be compiled
    """
