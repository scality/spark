from pyspark import SparkContext

def calculate_sum_of_squares():
    # Créer un contexte Spark
    sc = SparkContext.getOrCreate()

    # Exemple de données : une liste de nombres
    data = [1, 2, 3, 4, 5]
    data = range(1, 200000001)

    # Distribuer les données sur les workers
    rdd = sc.parallelize(data, 100)

    # Calculer le carré de chaque nombre
    squares = rdd.map(lambda x: x ** 2)

    # Somme des carrés
    sum_of_squares = squares.reduce(lambda a, b: a + b)

    # Retourner le résultat
    return sum_of_squares

if __name__ == "__main__":
    # Appeler la fonction de calcul
    result = calculate_sum_of_squares()

    # Afficher le résultat
    print(f"La somme des carrés est : {result}")
