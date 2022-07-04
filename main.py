import wrapper
import pandas as pd
if __name__ == '__main__':

    # READ INPUT FILE
    texts = list(pd.read_csv(r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian"
                             r"\script_directory_output\toxic_texts\toxic_multiscore_diego\testo_0.csv",
                             lineterminator="\n", low_memory=False, encoding="utf-8")["text"])
    # PERFORM SCORING
    score = wrapper.parallel_execution(texts)

    # WRITE PUTPUT FILE
    print("Writing output file...")
    score.to_csv(r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\script_directory_output"
                 "\toxic_texts\toxic_multiscore_diego\scored_text_0.csv", line_terminator="\n", index=False,
                 encoding="utf-8")
    print("Done!")

