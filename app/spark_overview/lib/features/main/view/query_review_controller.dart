import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:spark_overview/features/main/bloc/main_cubit.dart';

class QueryReviewController extends StatefulWidget {
  const QueryReviewController(
      {super.key, required this.title, required this.option});

  final String title;
  final int option;

  @override
  State<QueryReviewController> createState() => _QueryReviewControllerState();
}

class _QueryReviewControllerState extends State<QueryReviewController> {
  @override
  void initState() {
    super.initState();

    context.read<MainCubit>().getTitleRatingsData(widget.option);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
        appBar: AppBar(title: Text(widget.title)),
        body: BlocBuilder<MainCubit, MainState>(
          builder: (context, state) {
            if (state.status == Status.loading) {
              return const Center(child: CircularProgressIndicator());
            } else {
              return SingleChildScrollView(
                child: SizedBox(
                  width: double.maxFinite,
                  child: SingleChildScrollView(
                      scrollDirection: Axis.horizontal,
                      child: DataTable(
                          columns: state.data[0]
                              .map((e) => DataColumn(label: Text(e.toString())))
                              .toList(),
                          rows: state.data
                              .sublist(1)
                              .map((e) => DataRow(
                                  cells: e
                                      .map((e) => DataCell(Text(e.toString())))
                                      .toList()))
                              .toList())),
                ),
              );
            }
          },
        ));
  }
}
