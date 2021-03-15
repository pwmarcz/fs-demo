PLANTUML_VER = 1.2021.2
PLANTUML_URL = https://downloads.sourceforge.net/project/plantuml/$(PLANTUML_VER)/plantuml.$(PLANTUML_VER).jar
PLANTUML_SHA256 = d3a3eba987f509ce0564faf42b6c5473cdedfbfa866f03177bf04e57cd45bb17

DIAGRAMS = dentry.png write.png states.png

all: $(DIAGRAMS)

./%.png: %.uml $(PLANTUML)
	java -jar plantuml.jar $^

plantuml.jar:
	echo "PlantUML not found. Run 'make download'."

download:
	wget $(PLANTUML_URL) -O plantuml.$(PLANTUML_VER).jar.dl
	echo "$(PLANTUML_SHA256)  plantuml.$(PLANTUML_VER).jar.dl" | sha256sum -c
	mv plantuml.$(PLANTUML_VER).jar.dl plantuml.jar

