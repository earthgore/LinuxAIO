#include <stdio.h>
#include <stdlib.h>
#include <aio.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>


#define BUFFER_SIZE 4096 // Размер буфера, кратный размеру кластера
int source_fd, destination_fd;
off_t total_size;
int num_async_ops = 10; // Количество одновременных операций I/O
int inprocess = num_async_ops;
struct timespec start, finish;

struct aio_operation {
    struct aiocb aio;
    char *buffer;
    int write_operation;
    struct aio_operation* next_operation;
};
struct aio_operation *aiohead;

void aio_completion_handler(sigval_t sigval) {
    struct aio_operation *aio_op = (struct aio_operation *)sigval.sival_ptr;

    if (aio_op->write_operation)
    {
        // Операция записи завершена
        aio_op->write_operation = 0;
        aio_op->aio.aio_fildes = source_fd;
        aio_op->aio.aio_offset = aio_op->aio.aio_offset + aio_op->aio.aio_nbytes * num_async_ops;
        if(aio_op->aio.aio_offset >= total_size)
        {
            inprocess -= 1;
            if(inprocess == 0) raise(SIGUSR1);
        }
        else
        {
            if (aio_op->aio.aio_offset + aio_op->aio.aio_nbytes > total_size) {
                aio_op->aio.aio_nbytes = total_size - aio_op->aio.aio_offset;
            }
            if (aio_read(&aio_op->aio) == -1)              // Initiate the read operation.
            {
                perror("aio_read");
                exit(EXIT_FAILURE);
            }
        }
    }
    else
    {
        // Операция чтения завершена
        aio_op->write_operation = 1;
        aio_op->aio.aio_fildes = destination_fd;
        if (aio_write(&aio_op->aio) == -1)              // Initiate the read operation.
        {
            perror("aio_write");
            exit(EXIT_FAILURE);
        }
    }
}

void aio_operation_clean(struct aio_operation *aioptr)
{
    free(aioptr->buffer);
    if(aioptr->next_operation) aio_operation_clean(aioptr->next_operation);
    free(aioptr);
}

void SuccessFinishHandler(int signo){
    double elapsed;
    clock_gettime(CLOCK_MONOTONIC, &finish);
    elapsed = (finish.tv_sec - start.tv_sec);
    elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    printf("\nCopying was completed in %f seconds\n", elapsed);
    struct stat destination_stat;
    if (fstat(destination_fd, &destination_stat) == -1) {
        perror("fstat");
        close(source_fd);
        close(destination_fd);
        exit(EXIT_FAILURE);
    }
    printf("%lu\n", destination_stat.st_size);
    close(source_fd);
    close(destination_fd);
    aio_operation_clean(aiohead);
    exit(0);
}


struct aio_operation * aio_operation_setup(int fd, off_t offset, int size, int n) {
    if(n > 0)
    {
        struct aio_operation *aioptr = (struct aio_operation *) malloc(sizeof(aio_operation));;

        memset(&aioptr->aio, 0, sizeof(struct aiocb));
        char *buffer = (char *) malloc(BUFFER_SIZE * size);
        if (!buffer) {
            perror("malloc");
            close(source_fd);
            close(destination_fd);
            exit(EXIT_FAILURE);
        }
        aioptr->aio.aio_fildes = fd;
        aioptr->aio.aio_buf = buffer;
        aioptr->aio.aio_nbytes = BUFFER_SIZE * size;
        aioptr->aio.aio_offset = offset;
        aioptr->buffer = buffer;
        aioptr->write_operation = 0;
        aioptr->aio.aio_sigevent.sigev_notify = SIGEV_THREAD;
        aioptr->aio.aio_sigevent.sigev_notify_function = aio_completion_handler;
        aioptr->aio.aio_sigevent.sigev_value.sival_ptr = aioptr;
        aioptr->next_operation = aio_operation_setup(fd, offset + BUFFER_SIZE * size, size, n - 1);
        return aioptr;
    }
    else return nullptr;
}



int main() {
    const char *source_filename = "jean_delville-the_treasures_of_satan-1895-obelisk-art-history-1.jpg";
    const char *destination_filename = "write_file.jpg";

    int block_size = 8;

    struct stat source_stat;


    // Открытие файлов для чтения и записи
    source_fd = open(source_filename, O_RDONLY);
    if (source_fd == -1)
    {
        perror("open source");
        exit(EXIT_FAILURE);
    }

    destination_fd = open(destination_filename, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (destination_fd == -1) {
        perror("open destination");
        close(source_fd);
        exit(EXIT_FAILURE);
    }

    // Получение размера файла
    if (fstat(source_fd, &source_stat) == -1) {
        perror("fstat");
        close(source_fd);
        close(destination_fd);
        exit(EXIT_FAILURE);
    }

    total_size = source_stat.st_size;
    printf("%lu\n", total_size);

    // Инициализация структур для асинхронных операций
    aiohead = aio_operation_setup(source_fd, 0, block_size, num_async_ops);

    // Инициализация асинхронных операций чтения и записи
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (struct aio_operation *tmp = aiohead; tmp; tmp = tmp->next_operation)
    {
        if (aio_read(&tmp->aio) == -1)              // Initiate the read operation.
        {
            perror("aio_read");
            exit(EXIT_FAILURE);
        }
    }

    signal(SIGUSR1, SuccessFinishHandler);

    sleep(3600);

    return 0;
}
