export const BiLayers = ['Mode', 'Tableau'] as const;
export type BiLayer = typeof BiLayers[number];

export const parseBiLayer = (
  biLayer: string
): BiLayer => {
  const identifiedElement = BiLayers.find(
    (element) => element.toLowerCase() === biLayer.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};