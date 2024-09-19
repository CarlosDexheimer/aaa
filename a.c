#include "mpi.h"
#include <string.h>
#include <stdio.h>


int main(int argc, char *argv[]) {

    FILE *fp = fopen("spotify_millsongdata.csv", "r");

    if (!fp) {
        printf("Não consegui abrir o arquivo\n");
        return 1;
    }

    char buffer[50000];


    while (fgets(buffer, 50000, fp)) {
        const char* tok;
        for (tok = strtok(buffer, ","); tok && *tok; tok = strtok(NULL, ",\n")) {
            printf("%s\n", tok);
        }
    }

    fclose(fp);
    return 0;
}