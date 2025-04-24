import { Construct } from "constructs";

export interface BaseConstructProps {}

export class BaseConstruct extends Construct {
  private readonly props: Readonly<BaseConstructProps>;
  constructor(scope: Construct, id: string, props: BaseConstructProps) {
    super(scope, id);

    this.props = Object.freeze({ ...props });
  }

  // Pascal Caseへの変換
  protected toPascalCase(input: string): string {
    if (typeof input !== "string") {
      throw new Error("Input must be a string");
    }

    try {
      if (!input) return "";

      return (
        input
          // "-", "_" " "で単語ごとに分割
          .split(/[-_\s]+/)
          .map((word) => {
            // 空の単語をスキップ
            if (!word) return "";

            // 単語を文字配列に分解して処理
            const chars = word.split("");
            let prevIsUpper = false;

            // 各文字を処理
            const processedChars = chars.map((char, index) => {
              const isUpper = /[A-Z]/.test(char);

              // 最初の文字は大文字に変更
              if (index === 0) {
                prevIsUpper = isUpper;
                return char.toUpperCase();
              }

              // 現在の文字が大文字で、前の文字も大文字だった場合は小文字に変換
              if (isUpper && prevIsUpper) {
                prevIsUpper = true;
                return char.toLowerCase();
              }

              // それ以外の場合は元の状態を保持
              prevIsUpper = isUpper;
              return char;
            });

            return processedChars.join("");
          })
          .join("")
      );
    } catch (error) {
      console.error("Error converting to PascalCase:", error);
      throw error;
    }
  }
}
