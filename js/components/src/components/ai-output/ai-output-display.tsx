import { Platform, ScrollView, StyleSheet } from "react-native";
import { useAIOutputs } from "../../livestream-store";
import { Text, View } from "../ui";

export const AIOutputDisplay = () => {
  const aiOutputs = useAIOutputs();

  if (aiOutputs.length === 0) {
    return null;
  }

  const styles = StyleSheet.create({
    container: {
      position: Platform.OS === "web" ? ("fixed" as any) : "absolute",
      bottom: 20,
      right: 20,
      maxWidth: 400,
      maxHeight: 300,
      backgroundColor: "rgba(0, 0, 0, 0.8)",
      padding: 12,
      borderRadius: 8,
      zIndex: 1000,
    },
    title: {
      fontWeight: "bold" as any,
      marginBottom: 8,
      color: "white",
    },
    outputItem: {
      marginBottom: 8,
      padding: 8,
      backgroundColor: "rgba(255, 255, 255, 0.1)",
      borderRadius: 4,
    },
    text: {
      color: "white",
      fontFamily: Platform.OS === "web" ? "monospace" : undefined,
      fontSize: 14,
    },
    timestamp: {
      fontSize: 11,
      opacity: 0.6,
      marginTop: 4,
      color: "white",
    },
  });

  return (
    <View style={styles.container}>
      <Text style={styles.title}>AI Transcription</Text>
      <ScrollView>
        {aiOutputs.map((output, idx) => (
          <View key={idx} style={styles.outputItem}>
            {output.text ? (
              <Text style={styles.text}>{output.text}</Text>
            ) : (
              <Text style={styles.text}>
                {JSON.stringify(output.data, null, 2)}
              </Text>
            )}
            <Text style={styles.timestamp}>
              {new Date(output.timestamp).toLocaleTimeString()}
            </Text>
          </View>
        ))}
      </ScrollView>
    </View>
  );
};
