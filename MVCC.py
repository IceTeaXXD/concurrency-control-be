class MVCC:
    def __init__(self, input_sequence: str) -> None:
        self.sequence = []
        self.result = []
        self.transaction_history = []
        self.ctr = 0
        self.tx_ctr = [i for i in range(10)]
        self.version_table = {}

        try:
            if input_sequence[-1] == ';':
                input_sequence = input_sequence[:-1]
            input_sequence = input_sequence.split(';')
            for input in input_sequence:
                input = input.strip()
                if input[0] == 'R' or input[0] == 'W':
                    self.sequence.append(
                        {"operation": input[0], "transaction": int(input[1]), "table": input[3]})
                elif input[0] == 'C':
                    id = int(input[1])
                    self.sequence.append(
                        {"operation": input[0], "transaction": id})
                    # make sure that the transaction has a read or write operation
                    if len([x for x in self.sequence if x["transaction"] == id and (x["operation"] == 'R' or x["operation"] == 'W')]) == 0:
                        raise ValueError("Transaction has no read or write operation")
                else:
                    raise ValueError("Invalid operation detected")
            # Make sure that every read or write operation has a commit operation
            for x in self.sequence:
                if x["operation"] == 'R' or x["operation"] == 'W':
                    if len([y for y in self.sequence if y["transaction"] == x["transaction"] and y["operation"] == 'C']) == 0:
                        raise ValueError("Transaction has no commit operation")

            # Make sure every table is a single alphabet character, any symbol or number is not allowed
            if any(len(x["table"]) != 1 or not x["table"].isalpha() for x in self.sequence if x["operation"] == 'R' or x["operation"] == 'W'):
                raise ValueError("Invalid table name")

        except ValueError as e:
            raise ValueError(e)
        except Exception as e:
            raise ValueError(e)
        
    def maxWriteVersionIdx(self, tx, table):
        idx_max = next((i for i, entry in enumerate(self.version_table[table]) if entry['transaction'] == tx), 0)
        write_timestamp = self.version_table[table][idx_max]['timestamp'][1]
        for i, entry in enumerate(self.version_table[table]):
            if entry['timestamp'][1] > write_timestamp:
                idx_max = i
                write_timestamp = entry['timestamp'][1]
        return idx_max

    def read(self, tx, table):
        if table not in self.version_table.keys() and (('transaction', tx) not in self.version_table.items()):
            self.version_table[table] = []
            self.version_table[table].append({'transaction': tx, 'timestamp' : (self.tx_ctr[tx], 0), 'version': 0})
            self.result.append({'operation': 'R', 'transaction': tx, 'table': table, 'timestamp': (self.tx_ctr[tx],0), 'version': 0})
            self.ctr += 1
        else:
            idx_max = self.maxWriteVersionIdx(tx, table)
            read_timestamp, write_timestamp = self.version_table[table][idx_max]['timestamp']
            max_ver = self.version_table[table][idx_max]['version']
            if self.tx_ctr[tx] > read_timestamp:
                self.version_table[table][idx_max]['timestamp'] = (self.tx_ctr[tx], write_timestamp)
            self.result.append({'operation': 'R', 'transaction': tx, 'table': table, 'timestamp': self.version_table[table][idx_max]['timestamp'], 'version': max_ver})
            self.ctr += 1

    def write(self, tx, table):
        if table not in self.version_table.keys():
            self.version_table[table] = []
            self.version_table[table].append({'transaction': tx, 'timestamp' : (self.tx_ctr[tx], self.tx_ctr[tx]), 'version': self.tx_ctr[tx]})
            self.result.append({'operation': 'W', 'transaction': tx, 'table': table, 'timestamp': (self.tx_ctr[tx],self.tx_ctr[tx] ), 'version': self.tx_ctr[tx]})
            self.ctr += 1
        else:
            idx_max = self.maxWriteVersionIdx(tx, table)
            read_timestamp, write_timestamp = self.version_table[table][idx_max]['timestamp']
            max_ver = self.version_table[table][idx_max]['version']

            if self.tx_ctr[tx] < read_timestamp:
                self.result.append({'operation': 'W', 'transaction': tx, 'table': table, 'timestamp': (read_timestamp, self.tx_ctr[tx]), 'version': max_ver})
                self.tx_ctr[tx] = self.ctr
                self.result.append({'operation': 'rollback', 'transaction': tx, 'timestamp': self.tx_ctr[tx]})
                self.rollback(tx)

            elif self.tx_ctr[tx] == write_timestamp:
                self.version_table[table][idx_max]['timestamp'] = (read_timestamp, self.tx_ctr[tx])
                self.result.append({'operation': 'W', 'transaction': tx, 'table': table, 'timestamp': (read_timestamp, self.tx_ctr[tx]), 'version': max_ver})
                self.ctr += 1

            else: 
                self.version_table[table].append({'transaction': tx, 'timestamp' : (read_timestamp, self.tx_ctr[tx]), 'version': self.tx_ctr[tx]})
                self.result.append({'operation': 'W', 'transaction': tx, 'table': table, 'timestamp': (read_timestamp, self.tx_ctr[tx]), 'version': self.tx_ctr[tx]})
                self.ctr += 1

    def rollback(self, tx):
        tx_sequence = [op for op in self.result if op['transaction'] == tx and op['operation'] != 'rollback']
        tx_sequence += [op for op in self.sequence if op['transaction'] == tx]
        self.sequence = [op for op in self.sequence if op['transaction'] != tx]
        self.sequence += tx_sequence
        self.tx_ctr[tx] = self.ctr

    def commit(self, tx):
        self.result.append({'operation': 'commit', 'transaction': tx})

    def run(self):
        while len(self.sequence) > 0:
            current = self.sequence.pop(0)
            if current['operation'] == 'R':
                self.read(current['transaction'], current['table'])
            elif current['operation'] == 'W':
                self.write(current['transaction'], current['table'])
            elif current['operation'] == 'C':
                self.commit(current['transaction'])
            else:
                raise ValueError("Invalid operation detected")

    def __str__(self):
        res = ""
        for i in range(len(self.result)):
            if self.result[i]['operation'] == 'rollback':
                res += f"Transaction {self.result[i]['transaction']} rolled back with new timestamp {self.result[i]['timestamp']}.\n"
            elif self.result[i]['operation'] == 'commit':
                res += f"Transaction {self.result[i]['transaction']} committed.\n"
            elif self.result[i]['operation'] == 'R':
                res += f"Transaction {self.result[i]['transaction']} Read {self.result[i]['table']} at version {self.result[i]['version']}. Timestamp {self.result[i]['table']}: {self.result[i]['timestamp']}.\n"
            elif self.result[i]['operation'] == 'W':
                res += f"Transaction {self.result[i]['transaction']} Write {self.result[i]['table']} at version {self.result[i]['version']}. Timestamp {self.result[i]['table']}: {self.result[i]['timestamp']}.\n"
        return res
    
    def history_json(self):
        res = []
        for t in self.result:
            if t['operation'] == 'rollback':
                res.append({t['transaction']: f"rollback {t['timestamp']}."})
            elif t['operation'] == 'commit':
                res.append({t['transaction']: f"commit."})
            elif t['operation'] == 'R' or t['operation'] == 'W':
                res.append({t['transaction']: f"{t['operation']} {t['table']} at version {t['version']}. Timestamp {t['table']}: {t['timestamp']}."})
        return res
if __name__ == '__main__':
    try:
        mvcc = MVCC(input("Enter sequence (delimited by ;): "))
        mvcc.run()
        # print(mvcc)
        
        for res in mvcc.history_json():
            print(res)
    except Exception as e:
        print("Error: ", e)
        exit(1)
