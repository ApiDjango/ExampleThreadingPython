import threading
import example
handlers = {
        '3': lambda  task_id : example(task_id = task_id)
}

def addhandlers(number,task_id):
    handlers[number](task_id) 
    
def signal_handler(sig, frame):
    global exit_flag
    exit_flag = True
    print('Выход...')
    
# Функция, которая будет выполняться в каждом потоке
def run():
    global exit_flag
    
    # Запуск worker-потоков для обработки задач
    num_workers = 10  # Количество worker-потоков
    workers = []

    # Создание объекта конфигурации
    config = configparser.ConfigParser()

    # Загрузка файла INI
    config.read('config.ini')

    conn = psycopg2.connect(dbname=config.get('POSTGRES_DATABASE', 'database'), user=config.get('POSTGRES_DATABASE', 'user'), 
        password=config.get('POSTGRES_DATABASE', 'password'), host=config.get('POSTGRES_DATABASE', 'host'))
    cursor = conn.cursor()
    cursor.execute("select ....")
    results = cursor.fetchall()

    for i, row in enumerate(results):
        if i > 0 and i % num_workers == 0:
            # Ожидание завершения всех worker-потоков
            for worker_thread in workers:
                if worker_thread.is_alive():
                    worker_thread.join()
            workers = []

        worker_thread = threading.Thread(target=process_task, args=(conn, row))
        worker_thread.start()
        workers.append(worker_thread)

    # Ожидание завершения оставшихся worker-потоков
    for worker_thread in workers:
        if worker_thread.is_alive():
            worker_thread.join()
    
    cursor.close()
    conn.close()
    print('Работа окончена')    
    
if __name__ == '__main__':
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    run()    