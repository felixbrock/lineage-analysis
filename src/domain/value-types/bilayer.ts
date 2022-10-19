export const biLayers = ['Mode', 'Tableau', 'Metabase'] as const;
export type BiType = typeof biLayers[number];

export const parseBiLayer = (
  biLayer: string
): BiType => {
  const identifiedElement = biLayers.find(
    (element) => element.toLowerCase() === biLayer.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};