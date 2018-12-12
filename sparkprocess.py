import dateutil.parser
from dateutil import rrule
from datetime import datetime, timedelta

from pyspark.ml.feature import StopWordsRemover
import pyspark.sql.functions
from pyspark.sql.functions import split
import os


#Node 1
#now = dateutil.parser.parse('2018-12-06T18:00:00.000000Z')
#upto = dateutil.parser.parse('2018-12-08T08:00:00.000000Z')

#Node 2
#now = dateutil.parser.parse('2018-12-08T08:00:00.000000Z')
#upto = dateutil.parser.parse('2018-12-09T20:00:00.000000Z')

#Test
now = dateutil.parser.parse('2018-12-08T08:00:00.000000Z')
upto = dateutil.parser.parse('2018-12-08T08:59:00.000000Z')

for dt in rrule.rrule(rrule.HOURLY, dtstart=now, until=upto):
	range = df + timedelta(hours=1)
	print dt," to ",range
	

	## Hashtags ##
	
	spark = SparkSession.builder.getOrCreate()

	df = spark.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").option("dateFormat", "yyyy-MM-dd HH:mm:ss").option("inferSchema", "true").load("hdfs:///user/maria_dev/hashtag.csv")

	df.createOrReplaceTempView("hashtag")

	now = dt
	upto = range

	hashtags = sqlContext.sql("SELECT _c1 as hashtags, count(*) from hashtag where (_c0 BETWEEN '{}' AND '{}') group by hashtags order by count(*) desc limit 10".format(str(now),str(upto)))

	hdfsfile='/user/maria_dev/Results/Hashtags/hashtags'+dt.strftime('%Y-%m-%dT%H')
	hashtags.coalesce(1).write.csv(hdfsfile)

	os.system('mkdir -p /home/maria_dev/Results/Hashtags/hashtags'+dt.strftime('%Y-%m-%dT%H'))
	fullpath = os.popen('hadoop fs -ls '+hdfsfile+'/part*').read().split()[7]
	os.system('hadoop fs -get '+fullpath+' /home/maria_dev/Results/Hashtags/hashtags'+dt.strftime('%Y-%m-%dT%H')+'/')

	hashtags.show()


	# ## Users ##

	df = spark.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").option("dateFormat", "yyyy-MM-dd HH:mm:ss").option("inferSchema", "true").load("hdfs:///user/maria_dev/user.csv")

	df.createOrReplaceTempView("user")

	now = dt
	upto = range

	users = sqlContext.sql("SELECT _c1 as users, count(*) from user where (_c0 BETWEEN '{}' AND '{}') group by users order by count(*) desc limit 10".format(str(now),str(upto)))

	hdfsfile='/user/maria_dev/Results/Users/users'+dt.strftime('%Y-%m-%dT%H')
	users.coalesce(1).write.csv(hdfsfile)

	os.system('mkdir -p /home/maria_dev/Results/Users/users'+dt.strftime('%Y-%m-%dT%H'))
	fullpath = os.popen('hadoop fs -ls '+hdfsfile+'/part*').read().split()[7]
	os.system('hadoop fs -get '+fullpath+' /home/maria_dev/Results/Users/users'+dt.strftime('%Y-%m-%dT%H')+'/')

	users.show()


	# ## Occurrences ##

	df = spark.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").option("dateFormat", "yyyy-MM-dd HH:mm:ss").option("inferSchema", "true").load("hdfs:///user/maria_dev/keywords.csv")

	df.createOrReplaceTempView("occurrence")

	now = dt
	upto = range

	occurrences = sqlContext.sql("SELECT (case when _c1 like '%trump%' then 'trump' when _c1 like '%headache%' then 'headache' when _c1 like '%flu%' then 'flu' when _c1 like '%zika%' then 'zika' when _c1 like '%diarrhea%' then 'diarrhea' when _c1 like '%ebola%' then 'ebola' when _c1 like '%headache%' then 'headache' when _c1 like '%measles%' then 'measles' end) as occurrences, count(*) from occurrence where (_c0 BETWEEN '{}' AND '{}') group by occurrences order by count(*) desc".format(str(now),str(upto)))

	hdfsfile='/user/maria_dev/Results/Occurrences/occurrences'+dt.strftime('%Y-%m-%dT%H')
	occurrences.coalesce(1).write.csv(hdfsfile)

	os.system('mkdir -p /home/maria_dev/Results/Occurrences/occurrences'+dt.strftime('%Y-%m-%dT%H'))
	fullpath = os.popen('hadoop fs -ls '+hdfsfile+'/part*').read().split()[7]
	os.system('hadoop fs -get '+fullpath+' /home/maria_dev/Results/Occurrences/occurrences'+dt.strftime('%Y-%m-%dT%H')+'/')

	occurrences.show()


	# ## Keywords ##

	df = spark.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").option("dateFormat", "yyyy-MM-dd HH:mm:ss").option("inferSchema", "true").load("hdfs:///user/maria_dev/keywords.csv")

	df.createOrReplaceTempView("keyword")

	now = dt
	upto = range

	#keywords = sqlContext.sql("SELECT _c1 as keywords, count(*) from keyword where (_c0 BETWEEN '{}' AND '{}') group by keywords order by count(*) desc limit 100".format(str(now),str(upto)))

	keywords = sqlContext.sql("SELECT _c1 as keywords from keyword where (_c0 BETWEEN '{}' AND '{}')".format(str(now),str(upto)))

	stopwords_english = ["a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"]
	stopwords_spanish = ["0","1","2","3","4","5","6","7","8","9","_","a","actualmente","acuerdo","adelante","ademas","ademas","adrede","afirmo","agrego","ahi","ahora","ahi","al","algo","alguna","algunas","alguno","algunos","algun","alli","alli","alrededor","ambos","ampleamos","antano","antano","ante","anterior","antes","apenas","aproximadamente","aquel","aquella","aquellas","aquello","aquellos","aqui","aquel","aquella","aquellas","aquellos","aqui","arriba","arribaabajo","aseguro","asi","asi","atras","aun","aunque","ayer","anadio","aun","b","bajo","bastante","bien","breve","buen","buena","buenas","bueno","buenos","c","cada","casi","cerca","cierta","ciertas","cierto","ciertos","cinco","claro","comento","como","con","conmigo","conocer","conseguimos","conseguir","considera","considero","consigo","consigue","consiguen","consigues","contigo","contra","cosas","creo","cual","cuales","cualquier","cuando","cuanta","cuantas","cuanto","cuantos","cuatro","cuenta","cual","cuales","cuando","cuanta","cuantas","cuanto","cuantos","como","d","da","dado","dan","dar","de","debajo","debe","deben","debido","decir","dejo","del","delante","demasiado","demas","dentro","deprisa","desde","despacio","despues","despues","detras","detras","dia","dias","dice","dicen","dicho","dieron","diferente","diferentes","dijeron","dijo","dio","donde","dos","durante","dia","dias","donde","e","ejemplo","el","ella","ellas","ello","ellos","embargo","empleais","emplean","emplear","empleas","empleo","en","encima","encuentra","enfrente","enseguida","entonces","entre","era","erais","eramos","eran","eras","eres","es","esa","esas","ese","eso","esos","esta","estaba","estabais","estaban","estabas","estad","estada","estadas","estado","estados","estais","estamos","estan","estando","estar","estaremos","estara","estaran","estaras","estare","estareis","estaria","estariais","estariamos","estarian","estarias","estas","este","estemos","esto","estos","estoy","estuve","estuviera","estuvierais","estuvieran","estuvieras","estuvieron","estuviese","estuvieseis","estuviesen","estuvieses","estuvimos","estuviste","estuvisteis","estuvieramos","estuviesemos","estuvo","esta","estabamos","estais","estan","estas","este","esteis","esten","estes","ex","excepto","existe","existen","explico","expreso","f","fin","final","fue","fuera","fuerais","fueran","fueras","fueron","fuese","fueseis","fuesen","fueses","fui","fuimos","fuiste","fuisteis","fueramos","fuesemos","g","general","gran","grandes","gueno","h","ha","haber","habia","habida","habidas","habido","habidos","habiendo","habla","hablan","habremos","habra","habran","habras","habre","habreis","habria","habriais","habriamos","habrian","habrias","habeis","habia","habiais","habiamos","habian","habias","hace","haceis","hacemos","hacen","hacer","hacerlo","haces","hacia","haciendo","hago","han","has","hasta","hay","haya","hayamos","hayan","hayas","hayais","he","hecho","hemos","hicieron","hizo","horas","hoy","hube","hubiera","hubierais","hubieran","hubieras","hubieron","hubiese","hubieseis","hubiesen","hubieses","hubimos","hubiste","hubisteis","hubieramos","hubiesemos","hubo","i","igual","incluso","indico","informo","informo","intenta","intentais","intentamos","intentan","intentar","intentas","intento","ir","j","junto","k","l","la","lado","largo","las","le","lejos","les","llego","lleva","llevar","lo","los","luego","lugar","m","mal","manera","manifesto","mas","mayor","me","mediante","medio","mejor","menciono","menos","menudo","mi","mia","mias","mientras","mio","mios","mis","misma","mismas","mismo","mismos","modo","momento","mucha","muchas","mucho","muchos","muy","mas","mi","mia","mias","mio","mios","n","nada","nadie","ni","ninguna","ningunas","ninguno","ningunos","ningun","no","nos","nosotras","nosotros","nuestra","nuestras","nuestro","nuestros","nueva","nuevas","nuevo","nuevos","nunca","o","ocho","os","otra","otras","otro","otros","p","pais","para","parece","parte","partir","pasada","pasado","pais","peor","pero","pesar","poca","pocas","poco","pocos","podeis","podemos","poder","podria","podriais","podriamos","podrian","podrias","podra","podran","podria","podrian","poner","por","por que","porque","posible","primer","primera","primero","primeros","principalmente","pronto","propia","propias","propio","propios","proximo","proximo","proximos","pudo","pueda","puede","pueden","puedo","pues","q","qeu","que","quedo","queremos","quien","quienes","quiere","quiza","quizas","quiza","quizas","quien","quienes","que","r","raras","realizado","realizar","realizo","repente","respecto","s","sabe","sabeis","sabemos","saben","saber","sabes","sal","salvo","se","sea","seamos","sean","seas","segun","segunda","segundo","segun","seis","ser","sera","seremos","sera","seran","seras","sere","sereis","seria","seriais","seriamos","serian","serias","seais","senalo","si","sido","siempre","siendo","siete","sigue","siguiente","sin","sino","sobre","sois","sola","solamente","solas","solo","solos","somos","son","soy","soyos","su","supuesto","sus","suya","suyas","suyo","suyos","se","si","solo","t","tal","tambien","tambien","tampoco","tan","tanto","tarde","te","temprano","tendremos","tendra","tendran","tendras","tendre","tendreis","tendria","tendriais","tendriamos","tendrian","tendrias","tened","teneis","tenemos","tener","tenga","tengamos","tengan","tengas","tengo","tengais","tenida","tenidas","tenido","tenidos","teniendo","teneis","tenia","teniais","teniamos","tenian","tenias","tercera","ti","tiempo","tiene","tienen","tienes","toda","todas","todavia","todavia","todo","todos","total","trabaja","trabajais","trabajamos","trabajan","trabajar","trabajas","trabajo","tras","trata","traves","tres","tu","tus","tuve","tuviera","tuvierais","tuvieran","tuvieras","tuvieron","tuviese","tuvieseis","tuviesen","tuvieses","tuvimos","tuviste","tuvisteis","tuvieramos","tuviesemos","tuvo","tuya","tuyas","tuyo","tuyos","tu","u","ultimo","un","una","unas","uno","unos","usa","usais","usamos","usan","usar","usas","uso","usted","ustedes","v","va","vais","valor","vamos","van","varias","varios","vaya","veces","ver","verdad","verdadera","verdadero","vez","vosotras","vosotros","voy","vuestra","vuestras","vuestro","vuestros","w","x","y","ya","yo","z","el","eramos","esa","esas","ese","esos","esta","estas","este","estos","ultima","ultimas","ultimo","ultimos"]
	stopWords=stopwords_english+stopwords_spanish

	keywords = keywords.withColumn("keywords", split("keywords", "\s+"))
	remover = StopWordsRemover(inputCol="keywords", outputCol="filtered", stopWords=stopWords)
	keywords = remover.transform(keywords)
	keywords = keywords.groupBy('filtered').count().sort('count',ascending=False)
	keywords = keywords.withColumn('filtered', pyspark.sql.functions.concat_ws('|', 'filtered')).limit(20)

	hdfsfile='/user/maria_dev/Results/Keywords/keywords'+dt.strftime('%Y-%m-%dT%H')
	keywords.coalesce(1).write.csv(hdfsfile)

	os.system('mkdir -p /home/maria_dev/Results/Keywords/keywords'+dt.strftime('%Y-%m-%dT%H'))
	fullpath = os.popen('hadoop fs -ls '+hdfsfile+'/part*').read().split()[7]
	os.system('hadoop fs -get '+fullpath+' /home/maria_dev/Results/Keywords/keywords'+dt.strftime('%Y-%m-%dT%H')+'/')

	keywords.show()

