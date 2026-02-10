"""
    snakemake --cores 1
    snakemake --cores 1 --config species_filter="robin"
"""

# Configuration
configfile: "config.yaml"

# Get optional filter from config
SPECIES_FILTER = config.get("species_filter","")

# All rule - defines the final output
rule all:
    input:
        "output/bird_report.csv"

# Step 1: Scrape bird species data
rule scrape_species:
    output:
        flag="checkpoints/species_scraped.flag"
    log:
        "logs/scrape_species.log"
    shell:
        """
        python scripts/scrape_species.py > {log} 2>&1
        """

# Step 1b: Produce observations to Kafka
rule produce_kafka:
    output:
        flag="checkpoints/kafka_produced.flag"
    log:
        "logs/produce_kafka.log"
    shell:
        """
        python scripts/produce_kafka.py > {log} 2>&1
        """

# Step 2: Consume Kafka messages
rule consume_kafka:
    input:
        produce_flag="checkpoints/kafka_produced.flag"
    output:
        flag="checkpoints/kafka_consumed.flag"
    log:
        "logs/consume_kafka.log"
    shell:
        """
        python scripts/consume_kafka.py > {log} 2>&1
        """

# Step 3: Process audio files
rule process_audio:
    input:
        species_flag="checkpoints/species_scraped.flag"
    output:
        flag="checkpoints/audio_processed.flag"
    log:
        "logs/process_audio.log"
    shell:
        """
        python scripts/process_audio.py > {log} 2>&1
        """

# Step 4: Generate report
rule generate_report:
    input:
        kafka_flag="checkpoints/kafka_consumed.flag",
        audio_flag="checkpoints/audio_processed.flag"
    output:
        report="output/bird_report.csv"
    params:
        filter_arg=f" {SPECIES_FILTER}" if SPECIES_FILTER else ""
    log:
        "logs/generate_report.log"
    shell:
        """
        python scripts/generate_report.py{params.filter_arg} > {log} 2>&1
        """

# Clean rule to remove checkpoints and outputs
rule clean:
    shell:
        """
        rm -f checkpoints/*.flag
        rm -f output/*.csv
        rm -f logs/*.log
        echo "Cleaned checkpoints, outputs, and logs"
        """
