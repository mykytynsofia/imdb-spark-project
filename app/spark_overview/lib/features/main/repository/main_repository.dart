import 'package:csv/csv.dart';
import 'package:http/http.dart';

class MainRepository {
  Future<List<List<dynamic>>> getResultCSV(int option) async {
    try {
      final Response response = await get(
          Uri.parse(
              'https://storage.googleapis.com/imdb-dataframes-results/results/title.ratings/$option/result.csv'),
          headers: {
            "Access-Control-Allow-Origin": "*",
            'Content-Type': 'application/json',
            'Accept': '*/*'
          });

      if (response.statusCode == 200) {
        List<List<dynamic>> csv =
            const CsvToListConverter(fieldDelimiter: '|', eol: '\n')
                .convert(response.body);

        return csv;
      }

      return [];
    } catch (_) {
      rethrow;
    }
  }
}
