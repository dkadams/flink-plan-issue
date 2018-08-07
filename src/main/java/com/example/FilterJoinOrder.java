package com.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FilterJoinOrder {
  public static final class Element implements Serializable {
    public int value;
    public String requiredKey;
    public String optionalKey;

    public Element() {
    }

    public Element(int value, String requiredKey, String optionalKey) {
      this.value = value;
      this.requiredKey = requiredKey;
      this.optionalKey = optionalKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Element)) return false;
      Element element = (Element) o;
      return value == element.value &&
          Objects.equals(requiredKey, element.requiredKey) &&
          Objects.equals(optionalKey, element.optionalKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, requiredKey, optionalKey);
    }
  }

  public static final class Reference implements Serializable {
    private static final Reference NULL = new Reference(-1, null);

    public int id;
    public String key;

    public Reference() {
    }

    public Reference(int id, String key) {
      this.id = id;
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Reference)) return false;
      Reference reference = (Reference) o;
      return id == reference.id &&
          Objects.equals(key, reference.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, key);
    }
  }

  public static final class Reject implements Serializable {
    public Element element;
    public String reason;

    public Reject() {
    }

    public Reject(Element element, String reason) {
      this.element = element;
      this.reason = reason;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Reject)) return false;
      Reject reject = (Reject) o;
      return Objects.equals(element, reject.element) &&
          Objects.equals(reason, reject.reason);
    }

    @Override
    public int hashCode() {
      return Objects.hash(element, reason);
    }
  }

  public static final List<Element> ELEMENTS = Arrays.asList(
      new Element(5, "A", null),
      new Element(-1, "A", "A"),
      new Element(12, "B", "B"),
      new Element(10, null, null)
  );

  public static final List<Reference> REFERENCES = Arrays.asList(
      new Reference(100, "A"),
      new Reference(200, "C")
  );

  private static final OutputFormat<Reject> REJECT_SINK = new PrintingOutputFormat<>("REJECT", true);
  private static <T extends Serializable>
  DataSet<T> sinkRejects(DataSet<Either<T,Reject>> validated) {

    validated.flatMap(new FlatMapFunction<Either<T, Reject>, Reject>() {
      @Override
      public void flatMap(Either<T, Reject> value, Collector<Reject> out) throws Exception {
        if (value.isRight()) out.collect(value.right());
      }
    }).name("FilterRejects").output(REJECT_SINK).name("RejectSink");

    return validated.flatMap(new FlatMapFunction<Either<T, Reject>, T>() {
      @Override
      public void flatMap(Either<T, Reject> value, Collector<T> out) throws Exception {
        if (value.isLeft()) out.collect(value.left());
      }
    }).name("FilterValid");
  }

  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    final DataSet<Element> elements = env.fromCollection(ELEMENTS);
    final DataSet<Reference> reference = env.fromCollection(REFERENCES);

    // Validate the attributes of Element
    final DataSet<Either<Element, Reject>> attributeValidated
        = elements.map(new MapFunction<Element, Either<Element, Reject>>() {
      @Override
      public Either<Element, Reject> map(Element value) throws Exception {
        if (value.value < 1) {
          return Either.Right(new Reject(value, "Bad value"));
        }

        if (value.requiredKey == null) {
          return Either.Right(new Reject(value, "Missing required key"));
        }

        return Either.Left(value);
      }
    }).name("ValidateAttributes");
    final DataSet<Element> attributeValidElts = sinkRejects(attributeValidated);

    // Validate and enrich with Element.requiredKey
    final DataSet<Either<Tuple2<Element, Reference>, Reject>> requiredValidated =
        attributeValidElts.leftOuterJoin(reference)
            .where("requiredKey").equalTo("key")
            .with(new JoinFunction<Element, Reference, Either<Tuple2<Element, Reference>, Reject>>() {
              @Override
              public Either<Tuple2<Element, Reference>, Reject> join(Element elt, Reference ref) throws Exception {
                if (ref == null) return Either.Right(new Reject(elt, "Invalid required key"));
                return Either.Left(Tuple2.of(elt, ref));
              }
            }).name("ValidateRequiredKey");

    final DataSet<Tuple2<Element, Reference>> reqValidElts = sinkRejects(requiredValidated);

    // Extract the subset of elements with null optional key
    final DataSet<Tuple3<Element, Reference, Reference>> nullOptElts
        = reqValidElts.filter(new FilterFunction<Tuple2<Element, Reference>>() {
      @Override
      public boolean filter(Tuple2<Element, Reference> value) throws Exception {
        return value.f0.optionalKey == null;
      }
    }).name("FilterNullOptionalKey")
        .map(new MapFunction<Tuple2<Element, Reference>, Tuple3<Element, Reference, Reference>>() {
      @Override
      public Tuple3<Element, Reference, Reference> map(Tuple2<Element, Reference> value) throws Exception {
        return Tuple3.of(value.f0, value.f1, Reference.NULL);
      }
    }).name("EnrichNullOptionalReference");

    // Join the subset of elements with non-null optional keys to validate and enrich
    final FilterOperator<Tuple2<Element, Reference>> nonNullOptElts = reqValidElts.filter(new FilterFunction<Tuple2<Element, Reference>>() {
      @Override
      public boolean filter(Tuple2<Element, Reference> value) throws Exception {
        return value.f0.optionalKey != null;
      }
    });

    // Uncommenting the line below will resolve the issue, as it places the hash partition after the filter.
    //nonNullOptElts.printOnTaskManager("FIXED");

    final DataSet<Either<Tuple3<Element, Reference, Reference>, Reject>> optionalValidated =
        nonNullOptElts.name("FilterNonNullOptionalKey")
            .leftOuterJoin(reference)
            .where("f0.optionalKey").equalTo("key")
            .with(new JoinFunction<Tuple2<Element, Reference>, Reference, Either<Tuple3<Element, Reference, Reference>, Reject>>() {
              @Override
              public Either<Tuple3<Element, Reference, Reference>, Reject>
              join(Tuple2<Element, Reference> enrichedElt, Reference ref) throws Exception {
                if (ref == null) return Either.Right(new Reject(enrichedElt.f0, "Invalid optional key"));

                return Either.Left(Tuple3.of(enrichedElt.f0, enrichedElt.f1, ref));
              }
            }).name("ValidateOptionalKey");
    final DataSet<Tuple3<Element, Reference, Reference>> validOptElements = sinkRejects(optionalValidated);

    // Combine the null and validated optional key elements and sink to output.
    final DataSet<Tuple3<Element, Reference, Reference>> allValidElements
        = nullOptElts.union(validOptElements).name("UnionValidElements");

    final OutputFormat<Tuple3<Element, Reference, Reference>> validSink
        = new PrintingOutputFormat<>("VALID", false);
    allValidElements.output(validSink).name("ValidSink");

    System.out.println("env.getExecutionPlan() = " + env.getExecutionPlan());

    env.execute();
  }
}
