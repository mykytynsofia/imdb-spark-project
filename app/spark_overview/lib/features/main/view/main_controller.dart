import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:spark_overview/app/utils/queries.dart';
import 'package:spark_overview/features/main/bloc/main_cubit.dart';
import 'package:spark_overview/features/main/view/query_review_controller.dart';

class MainController extends StatelessWidget {
  const MainController({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
        appBar: AppBar(title: const Text('IMDB Spark Overview')),
        body: SizedBox(
          width: double.maxFinite,
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              const Text('Title Ratings Dataframe',
                  style: TextStyle(fontSize: 18, fontWeight: FontWeight.w700)),
              ...titleRatingsQueries
                  .map((query) => Container(
                      width: MediaQuery.sizeOf(context).width / 2,
                      margin: const EdgeInsets.all(10),
                      decoration: BoxDecoration(
                        color: Colors.white,
                        borderRadius: BorderRadius.circular(12),
                        boxShadow: const [
                          BoxShadow(
                              color: Colors.black12,
                              offset: Offset(0, 2),
                              blurRadius: 4)
                        ],
                      ),
                      child: ClipRRect(
                        borderRadius: BorderRadius.circular(12),
                        child: Material(
                          child: InkWell(
                              onTap: () {
                                context.read<MainCubit>().clear();
                                Navigator.of(context).push(MaterialPageRoute(
                                    builder: (context) => QueryReviewController(
                                        title: query,
                                        option: titleRatingsQueries
                                            .indexOf(query))));
                              },
                              child: Padding(
                                padding: const EdgeInsets.all(15),
                                child: Text(query,
                                    style: const TextStyle(fontSize: 16)),
                              )),
                        ),
                      )))
                  .toList()
            ],
          ),
        ));
  }
}
