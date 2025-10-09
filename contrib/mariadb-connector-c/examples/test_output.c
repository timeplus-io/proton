#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#define popen _popen
#define pclose _pclose
#endif
int main(int argc, char *argv[])
{
  char cmd_output[1024];
  char cmd_exp[1024];
  FILE *fp_exec, *fp_out, *fp_exp;

  if (argc < 3)
  {
    printf("Syntax: test_output test output expected");
    exit(-1);
  }
  if (!(fp_exec= popen(argv[1], "r")))
  {
    printf("Failed to run %s\n", argv[1]);
    exit(-1);
  }

  if (!(fp_out= fopen(argv[2], "w")))
  {
    printf("Failed to open %s for write\n", argv[2]);
    exit(-1);
  }

  while (NULL != fgets(cmd_output, sizeof(cmd_output-1), fp_exec))
  {
    fputs(cmd_output, fp_out);
  }
  pclose(fp_exec);
  fflush(fp_out);
  fclose(fp_out);

  if (argc == 3)
    return 0;

  if (!(fp_exp= fopen(argv[3], "r")))
  {
    /* if no exp file exists, we just return
       without an error = skip check */
    return(0);
  }
  if (!(fp_out= fopen(argv[2], "r")))
  {
    printf("Failed to open %s for read\n", argv[2]);
    exit(-1);
  }

  while (fgets(cmd_exp, sizeof(cmd_exp)-1, fp_exp))
  {
    if (!fgets(cmd_output, sizeof(cmd_output)-1, fp_out))
    {
      printf("Can't read from output file\n");
      goto error;
    }
    if (strcmp(cmd_output, cmd_exp))
    {
      printf("output and expected output are different\n");
      goto error;
    }
  }
  return 0;
error:
  fclose(fp_exp);
  fclose(fp_out);
  return 1;
}
