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
        self.result.append(cmd)

    def write(self, cmd):
        self.current_timestamp += 1
        tx_id = cmd['transaction']
        self.transactions[tx_id].writes.add(cmd['table'])
        self.transaction_history.append(
            {"operation": cmd['operation'], "transaction": tx_id, "table": cmd['table'], "status": "success"})
        self.result.append(cmd)

    def validate(self, cmd):
        self.current_timestamp += 1
        tx_id = cmd['transaction']
        self.transactions[tx_id].timestamps['validation'] = self.current_timestamp

        for ti_id, ti in self.transactions.items():
            if ti_id != tx_id and ti.timestamps['validation'] < self.transactions[tx_id].timestamps['validation']:
                if ti.timestamps['finish'] >= self.transactions[tx_id].timestamps['start'] and (ti.timestamps['finish'] < self.transactions[tx_id].timestamps['validation'] or self.transactions[tx_id].timestamps['validation'] == math.inf):
                    if any(write_item in self.transactions[tx_id].reads for write_item in ti.writes):
                        self.transaction_history.append({"operation": f"Abort due to conflict with {ti.tx_id}", "transaction": tx_id, "status": "aborted"})
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
        self.transactions[tx_id].status = "Aborted"
        # add all the the tx_id's operations to the back of the sequence
        for cmd in self.result:
            if cmd['transaction'] == tx_id:
                self.sequence.append(cmd)
        self.sequence.append({"operation": 'C', "transaction": tx_id})
        # clear the transaction's read and write sets
        self.transactions[tx_id].reads.clear()
        self.transactions[tx_id].writes.clear()
        # clear the transaction's timestamps
        self.transactions[tx_id].timestamps['start'] = math.inf
        self.transactions[tx_id].timestamps['validation'] = math.inf
        self.transactions[tx_id].timestamps['finish'] = math.inf

    def run(self):
        while len(self.sequence) > 0:
            cmd = self.sequence.pop(0)
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
                res += f"{cmd['operation']}{cmd['transaction']}\n"
            elif cmd['status'] == 'aborted':
                res += f"T{cmd['transaction']} {cmd['operation']}\n"
        return res


if __name__ == '__main__':
    try:
        input_sequence = input("Enter the sequence: ")
        occ = OCC(input_sequence)
        occ.run()
        print(occ)
    except Exception as e:
        print("Error:", e)
