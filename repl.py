"""
In memory key store implementation using python
for single user
 --Supports nested transaction
 Complexity:O(1)

Author:Dileep Kumar
Email:dileepkpde@gmail.com

"""

class imk():
    """
     In-memory key store class
    """
    def __init__(self):
        self.imk_store={}
        self.tran_log_stack=[]
        self.delete_key=[]
        self.imk_tran_store={}
        self.log_seq_num=0
        self.CURR_SAVE_POINT=None

    def start(self):
        prev_save_point=self.CURR_SAVE_POINT
        self.CURR_SAVE_POINT='T'+str(self.log_seq_num)
        self.tran_log_stack.append(self.CURR_SAVE_POINT)
        self.log_seq_num +=1
        self.imk_tran_store[self.CURR_SAVE_POINT]=self.imk_tran_store.get(prev_save_point) if prev_save_point else {}
        print(self.imk_tran_store)


    def commit(self):
        if len(self.imk_tran_store)>0:
            self.imk_store.update(self.imk_tran_store[self.CURR_SAVE_POINT])
            self.imk_tran_store.pop(self.CURR_SAVE_POINT)
            self.tran_log_stack.pop()
            if len(self.tran_log_stack)>0:
                self.CURR_SAVE_POINT=self.tran_log_stack[-1]
            else:
                self.CURR_SAVE_POINT=None
        else:
            print("Transaction not found")
    def abort(self):

        if len(self.imk_tran_store)>0:
            self.imk_tran_store.pop(self.CURR_SAVE_POINT)
            self.tran_log_stack.pop()
            if len(self.tran_log_stack)>0:
                self.CURR_SAVE_POINT=self.tran_log_stack[-1]
            else:
                self.CURR_SAVE_POINT=None

    def read( self, input_list :list):
        """

        :param input_list:
        :return:
        """
        if len(input_list) <2:
            print("Syntax error: Please provide key. Ex READ key1")
        else:
            key = input_list[1]
            if self.CURR_SAVE_POINT:
                result = self.imk_tran_store.get(self.CURR_SAVE_POINT).get(key)

            else:
                result =self.imk_store.get(key)
            if result:
                print(result)
            else:
                print("'{0}' Key not found".format(key))

    def write(self, input_list :list):
        """

        :param input_list:
        :return:
        """
        if len(input_list) <3:
            print("Syntax error: Please provide key and value. Ex WRITE key1 value1")
        else:
            key=input_list[1]
            value=' '.join(input_list[2:])
            if self.CURR_SAVE_POINT:
                self.imk_tran_store.get(self.CURR_SAVE_POINT)[key]=value
            else:
                self.imk_store[key]=value

    def delete(self, input_list :list):
        """

        :param input_list:
        :return:
        """
        if len(input_list) <2:
            print("Syntax error: Please provide key. Ex DELETE key1")
        else:
            key = input_list[1]
            if self.CURR_SAVE_POINT:
                imk_dictionary=self.imk_tran_store.get(self.CURR_SAVE_POINT)
            else:
                imk_dictionary=self.imk_store
            val=imk_dictionary.get(key)
            if val:
                imk_dictionary.pop(key)
            else:
                print("{0} Key not found".format(key))

def main():
    ALLOWED_COMMANDS=['WRITE','DELETE', 'READ','START','COMMIT','ABORT','QUIT']

    im=imk()
    print("In memory key store is started.")

    while True:
        print("imk>", end='')
        input_line=input()
        input_list=input_line.split(' ')
        command=input_list[0].upper()
        if command not in ALLOWED_COMMANDS:
            print("'{0}' command not recognized,allowed commands are {1}".format(command,','.join(ALLOWED_COMMANDS)))
        else:

            if command == 'WRITE':
                im.write(input_list)
            elif command == 'READ':
                im.read(input_list)
            elif command == 'DELETE':
                im.delete(input_list)
            elif command == 'START':
                im.start()
            elif command == 'COMMIT':
                im.commit()
            elif command == 'ABORT':
                im.abort()
            elif command == 'QUIT':
                print("Exiting imk..Bye!!")
                break


if __name__ =="__main__":
    main()
