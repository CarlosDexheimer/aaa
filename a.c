#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define MAX_LINE_LENGTH 4096
#define MAX_FIELD_LENGTH 3064
#define MAX_WORD_LENGTH 256
#define MAX_ARTISTS 100

typedef struct {
    char word[MAX_WORD_LENGTH];
    int count;
} WordCount;

typedef struct {
    char artist[MAX_FIELD_LENGTH];
    int song_count;
} ArtistCount;

int compare_word_count(const void *a, const void *b) {
    return strcmp(((WordCount *)a)->word, ((WordCount *)b)->word);
}

int compare_artist_count(const void *a, const void *b) {
    return ((ArtistCount *)b)->song_count - ((ArtistCount *)a)->song_count;
}

void count_words(const char *lyrics, WordCount *wordCounts, int *wordCountSize) {
    char tempLyrics[MAX_LINE_LENGTH];
    strcpy(tempLyrics, lyrics);
    char *token = strtok(tempLyrics, " ,.!?;:\"'()[]{}<>-");

    while (token != NULL) {
        int found = 0;
        for (int i = 0; i < *wordCountSize; i++) {
            if (strcmp(wordCounts[i].word, token) == 0) {
                wordCounts[i].count++;
                found = 1;
                break;
            }
        }
        if (!found && *wordCountSize < MAX_LINE_LENGTH) {
            strcpy(wordCounts[*wordCountSize].word, token);
            wordCounts[*wordCountSize].count = 1;
            (*wordCountSize)++;
        }
        token = strtok(NULL, " ,.!?;:\"'()[]{}<>-");
    }
}

void parse_csv_line(const char *line, ArtistCount *artistCounts, int *artistCountSize, WordCount *wordCounts, int *wordCountSize) {
    int i = 0, j = 0;
    char artist[MAX_FIELD_LENGTH];

    while (line[i] != ',' && line[i] != '\0') {
        artist[j++] = line[i++];
    }
    artist[j] = '\0';
    i++;

    char lyrics[MAX_LINE_LENGTH];
    j = 0;
    while (line[i] != '\0') {
        lyrics[j++] = line[i++];
    }
    lyrics[j] = '\0';

    count_words(lyrics, wordCounts, wordCountSize);

    int found = 0;
    for (int k = 0; k < *artistCountSize; k++) {
        if (strcmp(artistCounts[k].artist, artist) == 0) {
            artistCounts[k].song_count++;
            found = 1;
            break;
        }
    }
    if (!found && *artistCountSize < MAX_ARTISTS) {
        strcpy(artistCounts[*artistCountSize].artist, artist);
        artistCounts[*artistCountSize].song_count = 1;
        (*artistCountSize)++;
    }
}

int main(int argc, char *argv[]) {
    printf("iniciou o código\n");
    fflush(stdout);

    int mpi_init_result = MPI_Init(&argc, &argv);
    if (mpi_init_result != MPI_SUCCESS) {
        fprintf(stderr, "Erro ao inicializar o MPI\n");
        exit(1);
    }

    printf("iniciou o MPI\n");
    fflush(stdout);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 2) {
        if (rank == 0) {
            fprintf(stderr, "Uso: %s <nome_do_arquivo.csv>\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    FILE *file;
    if (rank == 0) {
        file = fopen(argv[1], "r");
        if (!file) {
            fprintf(stderr, "Não consegui abrir o arquivo %s\n", argv[1]);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        printf("leu o arquivo\n");
    }


    char line[MAX_LINE_LENGTH];
    ArtistCount artistCounts[MAX_ARTISTS] = {0};
    int artistCountSize = 0;
    WordCount wordCounts[MAX_LINE_LENGTH] = {0};
    int wordCountSize = 0;

    while (1) {
        if (rank == 0) {
            if (fgets(line, sizeof(line), file) == NULL) {
                strcpy(line, "");
            }
        }

        MPI_Bcast(line, MAX_LINE_LENGTH, MPI_CHAR, 0, MPI_COMM_WORLD);

        if (strlen(line) == 0) break;

        parse_csv_line(line, artistCounts, &artistCountSize, wordCounts, &wordCountSize);
    }

    if (rank == 0) fclose(file);

    WordCount globalWordCounts[MAX_LINE_LENGTH] = {0};
    int globalWordCountSize = 0;

    for (int i = 0; i < wordCountSize; i++) {
        int found = 0;
        for (int j = 0; j < globalWordCountSize; j++) {
            if (strcmp(globalWordCounts[j].word, wordCounts[i].word) == 0) {
                globalWordCounts[j].count += wordCounts[i].count;
                found = 1;
                break;
            }
        }
        if (!found && globalWordCountSize < MAX_LINE_LENGTH) {
            strcpy(globalWordCounts[globalWordCountSize].word, wordCounts[i].word);
            globalWordCounts[globalWordCountSize].count = wordCounts[i].count;
            globalWordCountSize++;
        }
    }

    ArtistCount *globalArtistCounts = malloc(MAX_ARTISTS * size * sizeof(ArtistCount));
    if (!globalArtistCounts) {
        printf("Falha ao alocar memória para globalArtistCounts\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Gather(artistCounts, artistCountSize * sizeof(ArtistCount), MPI_BYTE,
               globalArtistCounts, artistCountSize * sizeof(ArtistCount), MPI_BYTE,
               0, MPI_COMM_WORLD);

    if (rank == 0) {
        int globalArtistCountSize = 0;
        for (int i = 0; i < artistCountSize; i++) {
            int found = 0;
            for (int j = 0; j < globalArtistCountSize; j++) {
                if (strcmp(globalArtistCounts[j].artist, artistCounts[i].artist) == 0) {
                    globalArtistCounts[j].song_count += artistCounts[i].song_count;
                    found = 1;
                    break;
                }
            }
            if (!found && globalArtistCountSize < MAX_ARTISTS * size) {
                strcpy(globalArtistCounts[globalArtistCountSize].artist, artistCounts[i].artist);
                globalArtistCounts[globalArtistCountSize].song_count = artistCounts[i].song_count;
                globalArtistCountSize++;
            }
        }

        qsort(globalWordCounts, globalWordCountSize, sizeof(WordCount), compare_word_count);
        printf("Contagem de Palavras:\n");
        for (int i = 0; i < globalWordCountSize; i++) {
            printf("%s: %d\n", globalWordCounts[i].word, globalWordCounts[i].count);
        }
        printf("--------------------------------\n");

        qsort(globalArtistCounts, globalArtistCountSize, sizeof(ArtistCount), compare_artist_count);
        printf("Artistas com Mais Músicas:\n");
        for (int i = 0; i < globalArtistCountSize; i++) {
            printf("%s: %d\n", globalArtistCounts[i].artist, globalArtistCounts[i].song_count);
        }

        free(globalArtistCounts);
    }

    MPI_Finalize();
    return 0;
}
