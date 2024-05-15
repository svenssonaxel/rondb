#include "Keywords.hpp"

using std::cout;
using std::endl;

int
main()
{
  // Test that the list of implemented keywords is correctly sorted and only
  // contains strings with uppercase A-Z and underscore characters of correct
  // length.
  for (int i=0; i < number_of_keywords_implemented_in_rondb_sql; i++)
  {
    auto this_word = keywords_implemented_in_rondb_sql[i];
    int len = strlen(this_word.text);
    assert(1 <= len && len <= max_strlen_for_keyword_implemented_in_rondb_sql);
    for (int j=0; j < len; j++)
    {
      char c = this_word.text[j];
      assert(('A' <= c && c <= 'Z') || c == '_' );
    }
    if (i > 0)
    {
      auto prev_word = keywords_implemented_in_rondb_sql[i-1];
      assert(strcmp(prev_word.text, this_word.text) < 0);
    }
  }
  // Test that the list of keywords defined in MySQL is correctly sorted and
  // only contains strings with uppercase A-Z, digits 0-9 and underscore
  // characters.
  for (int i=0; i < number_of_keywords_defined_in_mysql; i++)
  {
    const char* this_word = keywords_defined_in_mysql[i];
    for (int j=0; this_word[j] != char(0); j++)
    {
      char c = this_word[j];
      assert(('0' <= c && c <= '9') || ('A' <= c && c <= 'Z') || c == '_' );
    }
    if (i > 0)
    {
      const char* prev_word = keywords_defined_in_mysql[i-1];
      assert(strcmp(prev_word, this_word) < 0);
    }
  }
  cout << "OK" << endl;
}
