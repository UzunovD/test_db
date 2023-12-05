from typing import List, Tuple, Union

NULL = 'NULL'


class DB:
    def __init__(self):
        self._db = {}
        self._value_data = {}
        self._is_transaction = False
        self._transaction_queries = []

    def set(self, query: List[str], commit=False):
        key, value = query
        if self._is_transaction and not commit:
            return self._transaction_queries.append({key: value})
        self._db[key] = value
        self._value_data.setdefault(value, set())
        self._value_data[value].add(key)

    def get(self, query: List[str]):
        key = query[0]
        res = self._db.get(key, NULL)
        if res == NULL and self._is_transaction and self._transaction_queries:
            res = self._transaction_queries[-1].get(key, None)
        print(res)

    def unset(self, query: List[str]):
        key = query[0]
        value = self._db.pop(key, None)
        self._value_data.get(value, set()).discard(key)

    def counts(self, query: List[str]):
        value = query[0]
        print(len(self._value_data.get(value, {})))

    def find(self, query: List[str]):
        value = query[0]
        print(', '.join(self._value_data.get(value, [NULL])))

    def _get_action(self, user_input: List[str]):
        command = user_input[0].lower()
        action = getattr(self, command, None)
        if not action or action == 'run' or command.startswith('_'):
            return None, 'Не опознанная команда'
        return action, None

    @staticmethod
    def _execute(action, user_input):
        try:
            result = action(user_input[1:] if user_input else None)
            if result:
                print(result)
        except ValueError:
            print('Атрибуты команды не корректны')

    @staticmethod
    def _get_user_input():
        user_input = input('Введите команду:\t').strip().split()
        if not user_input or user_input[0].lower() == 'end':
            raise KeyboardInterrupt
        return user_input

    def begin(self, _):
        self._is_transaction = True

    def rollback(self, _):
        if self._transaction_queries:
            self._transaction_queries.pop()
        if not self._transaction_queries:
            self._is_transaction = False

    def commit(self, _):
        query = self._transaction_queries.pop() if self._transaction_queries else None
        if query:
            self.set(list(map(list, query.items()))[0], commit=True)
        if not self._transaction_queries:
            self._is_transaction = False

    def run(self):
        while True:
            try:
                user_input = self._get_user_input()
            except KeyboardInterrupt:
                break
            action, msg = self._get_action(user_input)
            if msg:
                print(msg)
                continue
            self._execute(action, user_input)


if __name__ == '__main__':
    DB().run()
