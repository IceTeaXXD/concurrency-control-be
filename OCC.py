import math

class Transaction:
    def __init__(self, tx_id):
        self.tx_id = tx_id
        self.reads = set()
        self.writes = set()
        self.status = "Active"
        self.timestamps = {
            "start": math.inf,
            "validation": math.inf,
            "finish": math.inf
        }

    def __str__(self):
        return f"Transaction {self.tx_id}:\n\tRead Set: {self.reads}\n\tWrite Set: {self.writes}\n\tStatus: {self.status}\n\tTimestamps: {self.timestamps}"

class OCC:
    def __init__(self, input_sequence: str):
        self.sequence = []
        self.transactions = {}
        self.current_timestamp = 0
        self.result = []
        self.transaction_history = []

        try:
            if input_sequence[-1] == ';':
                input_sequence = input_sequence[:-1]
            input_sequence = input_sequence.split(';')
            for input_cmd in input_sequence:
                input_cmd = input_cmd.strip()
                if input_cmd[0] == 'R' or input_cmd[0] == 'W':
                    self.sequence.append(
                        {"operation": input_cmd[0], "transaction": int(input_cmd[1]), "table": input_cmd[3]})
                elif input_cmd[0] == 'C':
                    tx_id = int(input_cmd[1])
                    self.sequence.append(
                        {"operation": input_cmd[0], "transaction": tx_id})
                else:
                    raise ValueError("Invalid operation detected")

            if len([x for x in self.sequence if x["operation"] == 'C']) != len(set(x["transaction"] for x in self.sequence)):
                raise ValueError("Missing commit operation")

            if any(len(x["table"]) != 1 or not x["table"].isalpha() for x in self.sequence if x["operation"] == 'R' or x["operation"] == 'W'):
                raise ValueError("Invalid table name")

        except Exception as e:
            raise ValueError(e)

    def read(self, cmd):
        self.current_timestamp += 1
        tx_id = cmd['transaction']
        self.transactions[tx_id].reads.add(cmd['table'])
        self.transaction_history.append(
            {"operation": cmd['operation'], "transaction": tx_id, "table": cmd['table'], "status": "success"})

    def write(self, cmd):
        self.current_timestamp += 1
        tx_id = cmd['transaction']
        self.transactions[tx_id].writes.add(cmd['table'])
        self.transaction_history.append(
            {"operation": cmd['operation'], "transaction": tx_id, "table": cmd['table'], "status": "success"})

    def validate(self, cmd):
        self.current_timestamp += 1
        tx_id = cmd['transaction']
        self.transactions[tx_id].timestamps['validation'] = self.current_timestamp

        for ti_id, ti in self.transactions.items():
            if ti_id != tx_id and ti.timestamps['validation'] < self.transactions[tx_id].timestamps['validation']:
                if ti.timestamps['finish'] >= self.transactions[tx_id].timestamps['start'] and (
                        ti.timestamps['finish'] < self.transactions[tx_id].timestamps['validation'] or
                        self.transactions[tx_id].timestamps['validation'] == math.inf):
                    if any(write_item in self.transactions[tx_id].reads for write_item in ti.writes):
                        self.abort(tx_id)
                        return

        self.commit(tx_id)

    def commit(self, tx_id):
        self.current_timestamp += 1
        self.transactions[tx_id].timestamps['finish'] = self.current_timestamp
        for cmd in self.sequence:
            if cmd['transaction'] == tx_id:
                self.result.append(cmd)
        self.transaction_history.append(
            {"operation": 'C', "transaction": tx_id, "status": "commit"})
        self.transactions[tx_id].status = "Committed"

    def abort(self, tx_id):
        self.current_timestamp += 1
        self.transactions[tx_id].timestamps['finish'] = self.current_timestamp
        self.transaction_history.append(
            {"operation": 'C', "transaction": tx_id, "status": "aborted"})
        self.transactions[tx_id].status = "Aborted"
        # Rollback reads and writes
        for table in self.transactions[tx_id].reads:
            self.rollback_read(tx_id, table)
        for table in self.transactions[tx_id].writes:
            self.rollback_write(tx_id, table)

    def rollback_read(self, tx_id, table):
        print(f"Rolling back read operation for Transaction {tx_id} on Table {table}")

    def rollback_write(self, tx_id, table):
        print(f"Rolling back write operation for Transaction {tx_id} on Table {table}")

    def run(self):
        for cmd in self.sequence:
            tx_id = cmd['transaction']
            if tx_id not in self.transactions:
                self.transactions[tx_id] = Transaction(tx_id)
                self.transactions[tx_id].timestamps['start'] = self.current_timestamp

            if cmd['operation'] == 'R':
                self.read(cmd)
            elif cmd['operation'] == 'W':
                self.write(cmd)
            elif cmd['operation'] == 'C':
                self.validate(cmd)

            self.current_timestamp += 1

    def __str__(self):
        res = ""
        for cmd in self.transaction_history:
            if cmd['status'] == 'success':
                res += f"{cmd['operation']}{cmd['transaction']}({cmd['table']})\n"
            elif cmd['status'] == 'commit':
                res += f"{cmd['operation']}{cmd['transaction']} - commit\n"
            elif cmd['status'] == 'aborted':
                res += f"{cmd['operation']}{cmd['transaction']} - aborted\n"
        return res


if __name__ == '__main__':
    try:
        input_sequence = input("Enter the sequence: ")
        occ = OCC(input_sequence)
        occ.run()
        print(occ)
    except Exception as e:
        print("Error:", e)