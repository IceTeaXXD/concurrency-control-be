class TwoPhaseLocking:
    def __init__(self, input_sequence: str) -> None:
        self.sequence = []
        self.timestamp = []
        self.exclusive_lock_table = {}
        self.shared_lock_table = {}
        self.transaction_history = []
        self.result = []
        self.queue = []

        try:
            if input_sequence[-1] == ';':
                input_sequence = input_sequence[:-1]
            input_sequence = input_sequence.split(';')
            for input in input_sequence:
                input = input.strip()
                if input[0] == 'R' or input[0] == 'W':
                    self.sequence.append(
                        {"operation": input[0], "transaction": int(input[1]), "table": input[3]})
                    if int(input[1]) not in self.timestamp:
                        self.timestamp.append(int(input[1]))
                elif input[0] == 'C':
                    id = int(input[1])
                    self.sequence.append(
                        {"operation": input[0], "transaction": id})
                    # make sure that the transaction has a read or write operation
                    if id not in self.timestamp:
                        raise ValueError(
                            "Transaction has no read or write operation")

                else:
                    raise ValueError("Invalid operation detected")
            # Make sure that every transaction in the sequence has a commit
            if len([x for x in self.sequence if x["operation"] == 'C']) != len(set(self.timestamp)):
                raise ValueError("Missing commit operation")
            # Make sure every table is a single alphabet character, any symbol or number is not allowed
            if any(len(x["table"]) != 1 or not x["table"].isalpha() for x in self.sequence if x["operation"] == 'R' or x["operation"] == 'W'):
                raise ValueError("Invalid table name")

        except (ValueError) as e:
            raise ValueError(e)
        except Exception as e:
            raise ValueError(e)

    def shared_lock(self, transaction: int, table: str) -> bool:
        # Check if the table is locked
        if table in self.exclusive_lock_table:
            # Check if the table is locked by the same transaction
            if self.exclusive_lock_table[table] == transaction:
                return True
            else:
                return False
        else:  # Table is not exclusived locked
            # If the table is locked by itself, return True
            if table in self.shared_lock_table and transaction in self.shared_lock_table[table]:
                return True
            else:  # Check if the table is locked by another shared lock
                # Add the current transaction to the shared lock table
                if table not in self.shared_lock_table:
                    self.shared_lock_table[table] = []
                self.shared_lock_table[table].append(transaction)
                self.result.append(
                    {"operation": "SL", "transaction": transaction, "table": table})
                self.transaction_history.append({"transaction" : transaction, "table": table, "operation": "SL", "status": "Success"})
                return True

    def exclusive_lock(self, transaction: int, table: str) -> bool:
        # Check if the table is locked by a shared lock
        if table in self.shared_lock_table:
            # Check if the table is locked by the same transaction and the transaction is the only one in the shared lock table
            if transaction in self.shared_lock_table[table] and len(self.shared_lock_table[table]) == 1:
                # remove the shared lock
                self.shared_lock_table = {
                    k: v for k, v in self.shared_lock_table.items() if v != transaction}
                # add the transaction to the lock table
                self.exclusive_lock_table[table] = transaction
                self.result.append(
                    {"operation": "UPL", "transaction": transaction, "table": table})
                self.transaction_history.append({"transaction" : transaction, "table": table, "operation": "UPL", "status": "Success"})
                return True
            else:
                return False
        else:  # Check if the table is exclusive locked
            if table in self.exclusive_lock_table:
                # Check if the table is locked by the same transaction
                if self.exclusive_lock_table[table] == transaction:
                    return True
                else:
                    return False
            else:  # Table is not locked
                # Add the transaction to the lock table
                self.exclusive_lock_table[table] = transaction
                self.result.append(
                    {"operation": "XL", "transaction": transaction, "table": table})
                self.transaction_history.append({"transaction" : transaction, "table": table, "operation": "XL", "status": "Success"})
                return True

    def clear_shared_lock(self, current: dict) -> None:
        # get the table that is locked by the current transaction
        table = [
            k for k, v in self.shared_lock_table.items() if v == current["transaction"]]
        #  add the transaction to the transaction history
        for t in table:
            self.result.append(
                {"operation": "UL", "transaction": current["transaction"], "table": t})
            self.transaction_history.append({"transaction" : current["transaction"], "table": t, "operation": "UL", "status": "Success"})
        # remove the transaction from the lock table
        for k, v in self.shared_lock_table.items():
            if current["transaction"] in v:
                v.remove(current["transaction"])
        # if the table is empty, remove it from the lock table
        self.shared_lock_table = {
            k: v for k, v in self.shared_lock_table.items() if v != []}

    def clear_exclusive_lock(self, current: dict) -> None:
        if current["transaction"] in self.exclusive_lock_table.values():
            # get the table that is locked by the current transaction
            table = [
                k for k, v in self.exclusive_lock_table.items() if v == current["transaction"]]
            #  add the transaction to the transaction history
            for t in table:
                self.result.append(
                    {"operation": "UL", "transaction": current["transaction"], "table": t})
                self.transaction_history.append({"transaction" : current["transaction"], "table": t, "operation": "UL", "status": "Success"})
            # remove the transaction from the lock table
            self.exclusive_lock_table = {
                k: v for k, v in self.exclusive_lock_table.items() if v != current["transaction"]}

    def run_queue(self) -> None:
        while self.queue:
            transaction = self.queue.pop(0)
            # Check if the table is locked
            if self.exclusive_lock(transaction["transaction"], transaction["table"]):
                # add the transaction to the result
                self.result.append(transaction)
                self.transaction_history.append({"transaction" : transaction["transaction"], "table": transaction["table"], "operation": transaction["operation"], "status": "Success"})
            else:  # the table is locked
                # add the transaction back to the queue
                self.queue.insert(0, transaction)
                break

    def commit(self, current: dict) -> None:
        # check if any of the current transaction is still in the queue
        if current["transaction"] in [x["transaction"] for x in self.queue]:
            # move the current transaction to the second index of the sequence
            self.sequence.insert(1, current)
        else:
            # release the lock if any
            self.clear_shared_lock(current)
            self.clear_exclusive_lock(current)

            # add the transaction to the result
            self.result.append(current)
            self.transaction_history.append({"transaction" : current["transaction"], "table": "-", "operation": "Commit", "status": "Commit"})

    def abort(self, current: dict) -> None:
        self.transaction_history.append({"transaction": current["transaction"], "table": current["table"], "operation": "Abort", "status": "Abort"})
        # get all transaction that has the same transaction id
        curr = [x for x in self.result if x["transaction"] == current["transaction"] and (
            x["operation"] == 'R' or x["operation"] == 'W')]

        # remove the current transaction from the result
        self.result = [
            x for x in self.result if x["transaction"] != current["transaction"]]

        # get all transaction that has the same transaction id
        seq = [x for x in self.sequence if x["transaction"] == current["transaction"]]

        # remove the transaction from the sequence
        self.sequence = [
            x for x in self.sequence if x["transaction"] != current["transaction"]]

        # if the current transaction has a exclusive lock in the lock table, remove it
        if current["transaction"] in self.exclusive_lock_table.values():
            self.exclusive_lock_table = {
                k: v for k, v in self.exclusive_lock_table.items() if v != current["transaction"]}

        # if the current transaction has a shared lock in the lock table, remove it
        if current["transaction"] in [x for v in self.shared_lock_table.values() for x in v]:
            for k, v in self.shared_lock_table.items():
                if current["transaction"] in v:
                    v.remove(current["transaction"])
            self.shared_lock_table = {
                k: v for k, v in self.shared_lock_table.items() if v != []}

        # add the transaction to the end of the sequence
        self.sequence.extend(curr)
        self.sequence.append(current)
        self.sequence.extend(seq)

    def wait_die(self, current: dict) -> None:
        if ((current["table"] in self.exclusive_lock_table and self.timestamp.index(current["transaction"]) < self.timestamp.index(self.exclusive_lock_table[current["table"]])) or
                (current["table"] in self.shared_lock_table and all(self.timestamp.index(current["transaction"]) < self.timestamp.index(t) for t in self.shared_lock_table[current["table"]] if t != current["transaction"]))):
            # add the current transaction to the queue
            self.queue.append(current)
            self.transaction_history.append({"transaction": current["transaction"], "table": current["table"], "operation": current["operation"], "status": "Queue"})
        else:  # abort the current transaction
            self.abort(current)

    def run(self) -> None:
        while self.sequence:
            # Run the queue first
            self.run_queue()
            # Check the sequence
            # get the index of the first transaction in the sequence that does not exist in the queue
            index = next((i for i, x in enumerate(self.sequence) if x["transaction"] not in [
                y["transaction"] for y in self.queue]), None)
            # get the current transaction
            current = self.sequence.pop(index)

            # check if current is a commit
            if current["operation"] == 'C':
                self.commit(current)
            elif current["operation"] == 'R' and self.shared_lock(current["transaction"], current["table"]):
                self.result.append(current)
                self.transaction_history.append({"transaction": current["transaction"], "table": current["table"], "operation": current["operation"], "status": "Success"})
            elif current["operation"] == 'W' and self.exclusive_lock(current["transaction"], current["table"]):
                self.result.append(current)
                self.transaction_history.append({"transaction": current["transaction"], "table": current["table"], "operation": current["operation"], "status": "Success"})
            else:
                self.wait_die(current)

    def result_string(self) -> None:
        res = ""
        for r in self.result:
            if r["operation"] == 'C':
                res += f"{r['operation']}{r['transaction']};"
            else:
                res += f"{r['operation']}{r['transaction']}({r['table']});"
        if res[-1] == ';':
            res = res[:-1]
        return res

    def history_string(self):
        str = ""
        for t in self.transaction_history:
            str += f"{t['operation']} {t['transaction']} {t['table'] if 'table' in t else ''}\n"
        return str
    
    def history_json(self):
        res = []
        for t in self.transaction_history:
            res.append({t["transaction"]: f'{t["operation"]}({t["table"]})'})
        return res

if __name__ == "__main__":
    try:
        tpl = TwoPhaseLocking(input("Enter sequence (delimited by ;): "))
        tpl.run()
        print(tpl.result_string())
        for res in tpl.transaction_history:
            print(res)

    except (ValueError, IndexError) as e:
        print("Error: ", e)
        exit(1)

# R1(X);R2(X);R1(Y);C1;C2
# R1(A);R2(B);W1(A);R1(B);W3(A);W4(B);W2(B);R1(C);C1;C2;C3;C4
# R1(A);W2(A);R2(A);R3(A);W1(A);C1;C2;C3
# R1(X);W2(X);W2(Y);W3(Y);W1(X);C1;C2;C3
# R1(X);R2(Y);W1(Y);W1(X);W1(X);C1;C2
# R1(X);R2(X);W1(X);W2(X);W3(X);C1;C2;C3
# R1(X);R1(X);R2(X);R3(X);W1(X);W2(X);W3(X);C1;C2;C3
# R1(X);R2(X);W2(X);C1;C2
# R1(X);R2(X);W1(X);C1;C2
# R1(X);R2(X);R3(X);W1(X);W2(X);W3(X);C1;C2;C3
